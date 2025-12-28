### 1. 为什么用 Kafka？（核心对比）

假设场景：用户下单 -> 1.扣库存 -> 2.发邮件通知 -> 3.加积分。

#### ❌ 不用 Kafka (传统的同步/HTTP调用)
代码逻辑大概是：
```python
def create_order(order):
    save_to_db(order)
    inventory_service.decrease(order) # 耗时 50ms
    email_service.send(order)         # 耗时 200ms
    point_service.add(order)          # 耗时 50ms
    return "下单成功"
```
*   **痛点 1（慢）**：用户必须等所有步骤做完（300ms+）才能看到“下单成功”。
*   **痛点 2（脆）**：如果“邮件服务”挂了，整个下单接口直接报错，用户无法下单。
*   **痛点 3（乱）**：以后要加“发短信”功能，还得改这段代码，耦合度极高。

#### ✅ 使用 Kafka (异步/事件驱动)
代码逻辑变为：
```python
def create_order(order):
    producer.send('order_topic', order) # 耗时 5ms
    return "下单成功"
```
*   **优势 1（快）**：用户只需要 5ms 就能得到响应。
*   **优势 2（稳）**：邮件服务挂了？没关系，消息暂存在 Kafka 里，等邮件服务修好了，重启后自动接着消费，**数据不丢失**。
*   **优势 3（解耦）**：以后要加“发短信”，只需要新写一个脚本监听 Kafka，完全不用改动下单代码。

---

### 2. 极简 Python Kafka Demo

我们将实现 **1 个生产者（订单系统）** 和 **2 个不同的消费者（库存服务、邮件服务）**。

#### 🛠️ 环境准备
1.  **安装 Python 库**:
    ```bash
    pip install kafka-python
    ```
2.  **启动 Kafka (使用 Docker Compose)**:
    创建一个 `docker-compose.yml` 文件并运行 `docker-compose up -d`。
    ```yaml
    version: '3'
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.4.0
        environment:
          ZOOKEEPER_CLIENT_PORT: 2181
      kafka:
        image: confluentinc/cp-kafka:7.4.0
        depends_on: [zookeeper]
        ports: ['9092:9092']
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ```

---

#### 📄 代码实现

这里有三个 Python 脚本，分别代表三个独立的微服务。

#### 1. 生产者：订单系统 (`producer.py`)
它只负责疯狂接收订单，往 Kafka 里扔，不关心后续处理。

```python
import time
import json
import random
from kafka import KafkaProducer

# 初始化生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # 自动将字典转为JSON字节
)

print("🛒 订单系统启动，准备接单...")

order_id = 1
try:
    while True:
        # 模拟生成一个订单
        order = {
            "id": order_id,
            "user": f"User_{random.randint(1, 100)}",
            "amount": random.randint(10, 500),
            "ts": time.time()
        }
        
        # 发送消息到 'orders' 主题
        # 核心：send 是异步的，这里仅仅是把消息放入缓冲区
        producer.send('orders', order)
        
        print(f"✅ 订单 {order_id} 已发送 Kafka")
        
        order_id += 1
        time.sleep(1) # 模拟每秒产生一个订单
except KeyboardInterrupt:
    producer.close()
```

#### 2. 消费者 A：库存服务 (`consumer_inventory.py`)
这是核心业务，必须由一个消费者组处理。

```python
import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders', # 订阅的主题
    bootstrap_servers=['localhost:9092'],
    group_id='inventory_group',  # <--- 关键点：消费者组 ID
    auto_offset_reset='latest',  # 从最新的消息开始读
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📦 库存服务启动 (Group: inventory_group)...")

for message in consumer:
    data = message.value
    # 模拟业务逻辑处理
    print(f"📦 [库存扣减] 订单ID: {data['id']} - 商品准备出库...")
    # 这里不需要 sleep，库存处理通常很快
```

#### 3. 消费者 B：邮件通知服务 (`consumer_email.py`)
这是一个处理较慢的服务，我们模拟它处理很慢。**注意它的 group_id 和上面不同**，这意味着 Kafka 会把同一条消息**同时**发给库存和邮件（广播模式）。

```python
import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='email_group',  # <--- 注意：不同的 Group ID
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📧 邮件服务启动 (Group: email_group)...")

for message in consumer:
    data = message.value
    print(f"📧 [正在发送邮件] 给用户 {data['user']} ...")
    
    # 核心练手点：模拟这是一个慢服务
    time.sleep(2) 
    
    print(f"   -> 邮件发送完成 (订单 {data['id']})")
```

---

### 3. 如何练习与验证（这才是学习的关键）

请按照以下顺序操作，观察现象，理解 Kafka 的核心：

#### 实验一：解耦与广播 (Pub/Sub)
1.  打开 3 个终端窗口。
2.  分别运行 `python consumer_inventory.py` 和 `python consumer_email.py`。
3.  运行 `python producer.py`。
4.  **观察现象**：
    *   Producer 每秒发一个单。
    *   库存服务（Inventory）刷屏很快，实时跟进。
    *   邮件服务（Email）因为有 `sleep(2)`，处理得很慢。
    *   **结论**：邮件服务的慢，完全没有拖累生产者的发送速度，也没有影响库存服务的处理速度。这就是**解耦**。

#### 实验二：削峰填谷 (Buffering)
1.  保持 Producer 继续运行。
2.  **强行关闭** `consumer_email.py`（按 Ctrl+C），模拟邮件服务宕机。
3.  让 Producer 继续跑 10 秒钟（此时产生了 10 条新消息，邮件服务没收到）。
4.  **重启** `consumer_email.py`。
5.  **观察现象**：
    *   重启瞬间，邮件服务会疯狂打印刚才错过的 10 个订单，直到追上最新进度。
    *   **结论**：Kafka 充当了缓冲区。下游挂了没关系，消息存着，活过来再处理。

#### 实验三：消费者组负载均衡 (Consumer Groups)
*这是 Kafka 最强大的功能之一。*
1.  现在的邮件服务太慢了（每2秒处理1个，但生产是每1秒1个），消息会越积越多（Lag）。
2.  **不要关闭**原来的 `consumer_email.py`。
3.  新开一个终端，**再运行一次** `consumer_email.py`（此时有两个邮件服务实例，属于同一个 `email_group`）。
4.  **观察现象**：
    *   你会发现订单被**分摊**到了两个窗口中。
    *   窗口 1 处理奇数订单，窗口 2 处理偶数订单（大概率）。
    *   整体处理速度翻倍了！
    *   **结论**：这就是**水平扩展**。处理不过来？加机器（起新进程）就行，Kafka 自动把负载分配过去。

### 总结
这个 Python Demo 虽然代码很少，但完整演示了：
1.  **Producer** 非阻塞发送。
2.  **Broker** 存储消息。
3.  **Consumer** 使用不同的 `Group ID` 实现**广播**（库存和邮件都收到）。
4.  **Consumer** 使用相同的 `Group ID` 实现**抢单/负载均衡**（两个邮件服务分摊压力）。

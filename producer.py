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
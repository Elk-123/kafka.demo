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
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
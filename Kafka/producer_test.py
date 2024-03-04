import threading
import json

from kafka import KafkaProducer

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


class thread_job:
    def __init__(self, number):
        self.number = number


def test(number: int):
    for i in range(100):
        info = {'thread': number, 'order': i}
        
        message = producer.send('demo', info)

        # Chờ phản hồi
        record_metadata = message.get()

        # Kiểm tra phản hồi
        if record_metadata.topic == 'demo':
            print("Dữ liệu đã được gửi thành công lên topic")
        else:
            print("Lỗi: Dữ liệu không được gửi")


# main function
def main():
    job_list = []
    
    for i in range(3):
        job = thread_job(i)
        
        job_list.append(job)
        
    threads_list = []

    for job in job_list:
        thread = threading.Thread(target=test, args=(job.number,))

        threads_list.append(thread)
    
        thread.start()
        
    for thread in threads_list:
        thread.join()
        
    producer.flush()
    

main()

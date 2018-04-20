# driver.py
# Faustino Cortina
# This file contains test code to test the functionality of the project.


from Converter import transcode
from worker import Worker
import os


# Starts RabbitMQ server and initializes 3 Celery workers, with the third worker being considered 'fast' for the sake of this example

os.system("rabbitmq-server -detached")
os.system("celery multi start worker1@axle1 worker2@axle2 -Q:1 'worker1' -Q:2 'worker2' --pool=solo -A DConverter --logfile=/dev/null")

for q in [1,2]:
    worker_name = 'worker' + str(q)
    worker = Worker(worker_name, 'FFMPEG', 'No')
    worker.add_to_database()

worker = Worker('worker3','FFMPEG','No')
worker.add_to_database()



# Creates bogus tasks using 2 random video files to test out project

os.system("python SQLDatabase.py -i random.mp4 -o random.avi -b 64 -s 00:00:20 -d 00:00:25")

os.system("python SQLDatabase.py -i random.mp4 -o random1.avi -b 64/64")

os.system("python SQLDatabase.py -i random2.mp4 -o random2.avi -b 64/64 -s 00:00:20 -d 00:00:25")

os.system("python SQLDatabase.py -i random.mp4 -o random3.avi -b 64/64")

os.system("python SQLDatabase.py -i random2.mp4 -o random4.avi -b 64")

os.system("python SQLDatabase.py -i random.mp4 -o random5.avi -b 64/64")

os.system("python SQLDatabase.py -i random2.mp4 -o random6.avi -b 64/64 -s 00:00:20")

os.system("python SQLDatabase.py -i random.mp4 -o random7.avi")

os.system("python SQLDatabase.py -i random.mp4 -o random8.avi -b 64/64")

os.system("python SQLDatabase.py -i random2.mp4 -o random9.avi -b 64/64")

os.system("python SQLDatabase.py -i random2.mp4 -o random10.avi -b 64 -s 00:00:20 -d 00:00:25")

os.system("python SQLDatabase.py -i random.mp4 -o random11.avi -b 64/64")


# Runs the transcode program (see Converter.py for an explanation of the parameters)
transcode(15, 100)

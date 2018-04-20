import psycopg2


host = 'localhost'
user = 'postgres'
password = 'password'
DBName = 'postgres'

'''
host = 'localhost'
user = 'faustinocortina'
port = 5435
DBName = 'axle'
'''

class Worker:
    def __init__(self, name, transcoder, fastB, OS='osx', task=0, ID=0):
        self.name = repr(name)
        self.transcoder = repr(transcoder)
        self.fast = repr(fastB)
        self.os = repr(OS)
        self.id = repr(ID)
        self.task = repr(task)

    def add_to_database(self):

        try:

            axle = psycopg2.connect(host=host, user=user, password=password, dbname=DBName)

        except:

            print("Error Connecting to Axle Database")

        cur = axle.cursor()

        try:

            cur.execute("CREATE TABLE public.workers(queue varchar(255),transcoder varchar(255),fastB varchar(255),os varchar(255), task int, id int)")

        except:

            axle.rollback()

        cur.execute("INSERT INTO public.workers (queue, transcoder, fastB, OS, task, id) VALUES (%s,%s,%s,%s,0,0)" % (self.name, self.transcoder, self.fast, self.os))

        axle.commit()

        cur.close()

        axle.close()



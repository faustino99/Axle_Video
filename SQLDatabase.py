### SQLDatabase.py
### This python script is made to be executed on the command line and adds a task to a queue stored in an SQL database
### Usage: python SQLDatabase.py -i input_file -o output_file [-b bitrate -s start -d duration]
### where: input_file is to be transcoded to the output file specified, and bitrate, start and duration flags are optional
### Example: python SQLDatabase.py -i random.mp4 -o random.avi -b 64 -s 00:00:20 -d 00:00:45


import psycopg2, getopt, sys

host = 'localhost'
user = 'postgres'
password='password'
DBName = 'postgres'

'''
host = 'localhost'
user = 'faustinocortina'
port = 5435
DBName = 'axle'
'''
try:

	axle = psycopg2.connect(host=host, user=user, password=password, dbname=DBName)

except:
	
	print("Error Connecting to Axle Database")

cur = axle.cursor()

try:

	cur.execute("CREATE TABLE public.media(input varchar(255),output varchar(255),bitRate varchar(255),start varchar(255),duration varchar(255),status varchar(255),flag varchar(255),size varchar(255),id SERIAL UNIQUE)")

except:

	axle.rollback()


try:

	myopts, args = getopt.getopt(sys.argv[1:],"i:o:b:s:d:")

except:

	pass


inP = 'NULL'
outP = 'NULL'
bitR = 'NULL'
start = 'NULL'
duration = 'NULL'
status = "'INCOMPLETE'"
flag = 'NULL'
size = 'NULL'

for o, a in myopts:

	if o == '-i':

		inP = repr(a)

	elif o == '-o':

		outP = repr(a)

	elif o == '-b':

		bitR = repr(a)

	elif o == '-s':

		start = repr(a)

	elif o == '-d':

		duration = repr(a)

if (inP == 'NULL') or (outP == 'NULL'):

	print "ERROR: Incorrect Format"
	print "Correct Format: 'python SQLDatabase.py -i [input file] -o [output file]'"

else:

	cur.execute("INSERT INTO public.media (input,output,bitRate,start,duration,status,flag,size) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" % (inP,outP,bitR,start,duration,status,flag,size))

	axle.commit()

cur.close()

axle.close()



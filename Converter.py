# Converter.py
# Faustino Cortina
# This program completes transcoding tasks for Axle Video's proxy files as new media files added to Axle in real time
# The program uses information on task size and the speed of the available transcoding processes to complete queues of tasks as efficiently as possible




from DConverter import ffmpegVideo

import psycopg2, sys, time, atexit, getpass, getopt, subprocess, os, datetime, time

# Main function of the program


'''
This function reads the media table in Axle Video database for a list of pending media files to be transcoded 
Next the function reads the worker table to get details on the number of parallel processes available to transcode the queue.
Then the function assigns tasks to each process (called workers in the funtion), assigning any large tasks (task size > largeFileSize) to the faster 'workers'.
The function continuously checks the status of each worker and immediately assigns another task to a worker once it completes its previous task.
The function continues indefinitely, waiting for new tasks from the Axle database if all tasks are completed.
'''

def transcode(largeFileSize, largeFileOverloadCutoff):


    # PARAMETERS

    # largeFileSize: any task with a task size greater than the number (in MB) given by the largeFileSize parameter is considered a large file.
    # largeFileOverloadCutoff: If all the large tasks in the queue have a cumulative size greater than the number provided by the largeFileOverloadCutoff,
    # then the queue is considered overloaded, and any worker regardless of speed is allowed to transcode large tasks.

    largeFileSize = float(largeFileSize)
    largeFileOverloadCutoff = float(largeFileOverloadCutoff)


    # set to true if you want free workers that aren't considered fast to assist in transcoding large files if needed.
    freeWorkerLargeFile = True

    # Values used to connect to the Axle SQL Server
    # Test Database
    host = 'localhost'
    DBName = 'postgres'
    user = 'postgres'
    password = 'password'

    # Axle Database
    '''
    host = 'localhost'
    port = 5435
    DBName = 'axle'
    user = getpass.getuser() # name of current user
    '''

    ffmpegWorkers = []  ## list of workers who are using FFMPEG as their transcoder
    primeWorkers = []  ## list of workers who are using Prime as their transcoder
    linuxWorkers = []  ## list of workers who are using a Linux operating system (assumed the rest are using osx)
    fastWorkers = []  ## list of workers who are relatively faster than the other workers (ex: fast transcoder, fast GPU, etc.)

    try:

        axle = psycopg2.connect(host=host, user=user, dbname=DBName, password=password) # Used to connect to test database
        #axle = psycopg2.connect(host=host, user=user, dbname=DBName, port=port) # Used to connect to Axle database

        cur = axle.cursor()

    except:

        print("Unable to connect to Axle server")
        return

    # Grabs information on worker processes from Axle Database
    cur.execute("SELECT * FROM public.workers")
    workers_raw = cur.fetchall()
    workers = [list(i) for i in workers_raw]
    numWorkers = len(workers) # total number of workers

    fastQueue = False # If no fast workers are found in worker list, then there is no 'fast queue' for large files

    # Gathers information about each process (ex: speed, OS, transcoder type)
    for i in range(numWorkers):
        if workers[i][2] == 'Yes':
            fastQueue = True # Indicates there are fast processes available for large files
            fastWorkers.append(i)
        if workers[i][3] == 'linux':
            linuxWorkers.append(i)
        if workers[i][1] == 'FFMPEG':
            ffmpegWorkers.append(i)
        elif inputL[i][1] == 'Prime':
            primeWorkers.append(i)


    print ("\nStarting Axle Transcode")
    print ("Running %d simultaneous processes transcoding up to %d media files at a time" % (numWorkers,numWorkers))

    # Closes connections to Axle database when function is terminated
    def exit_handler():
        cur.close()
        axle.close()
        print ("\n***Ending Axle Transcode Script***\n")

    atexit.register(exit_handler)

    first_loop = True  # indicates that it is the first iteration of the while loop

    # Loop continues indefinitely, constantly monitoring Axle database for new tasks
    while True:

        queue = 0 # value of 0 means that all workers are busy

        # Finds a free worker to assign a task to later
        for z in range(numWorkers):

            if workers[z][4] == 0: # value of 0 means that the worker is available for a new job

                queue = workers[z][0] # queue is a unique id (ex: 'worker1') that each worker has, needed to differentiate between processes

                queueid = z # queueid stores the index of free worker in 'workers' list for future reference when assigning it a new task.

                break

        # If all workers are busy, function waits until a worker completes a task
        if queue == 0:

            PENDING = True

            while(PENDING):

                for w in range(numWorkers):

                    if(workers[w][4].status == 'SUCCESS'):

                        print ("\n%d task(s) left in queue (task complete by process #%s)\n" % (numTasks-1,str(w+1)))

                        workers[w][4] = 0 # value of 0 indicates worker is now free

                        cur.execute("DELETE FROM public.media WHERE id=%s" % (repr(workers[w][5]))) # Deletes completed task from database

                        axle.commit() # commits changes to database

                        workers[w][5] = 0

                        queue = workers[w][0]

                        queueid = w

                        PENDING = False

                        break

        WAITING = True

        # Loop ends when a free worker is assigned a task
        while (WAITING):

            # Grabs current list of pending tasks from database
            cur.execute("SELECT * FROM public.media")
            tasksT = cur.fetchall()
            tasks = [list(i) for i in tasksT]
            numTasks = len(tasks) # number of pending tasks in database


            for i in range(len(tasks)):

                # If not already done, makes a rough estimate on task size based on input file size & start and duration of output file.
                if tasks[i][7] == None:

                    videoSize = initializeVideoSize(tasks[i])
                    tasks[i][7] = str(videoSize) # updates task list with task size
                    id = tasks[i][8]

                    # Updates database with task size
                    cur.execute("UPDATE public.media SET size = %s WHERE id=%s" % (str(videoSize),repr(id))) # updates database with with task size
                    axle.commit()

            # If this is the first time the while loop is being executed, then all the workers are inactive (haven't been assigned a task)
            if first_loop and tasks != []:

                first_loop = False

                # Checks to see if there are any half-completed tasks from a previous execution of the function
                for l in range(len(tasks)):

                    if tasks[l][5] == 'PENDING':

                        # Sets any half-complete functions to incomplete to be re-done correctly
                        tasks[l][5] == 'INCOMPLETE'
                        cur.execute("UPDATE public.media SET status='INCOMPLETE' WHERE status='PENDING'")

                        # commits changes to the database
                        axle.commit()

            # assigns a task to the free worker
            for i in range(len(tasks)):

                totalLargeFileSize = largeFileQueueSize(tasks,largeFileSize) # adds up task size of all large jobs (in MB)

                totalSmallFileSize = smallFileQueueSize(tasks,largeFileSize) # adds up task size of all small jobs (in MB)

                try:

                    if tasks[i][5] == 'INCOMPLETE': # indicates task hasn't been assigned to a worker yet

                        # runs if free worker is fast
                        if queueid in fastWorkers:

                            # if there aren't any large tasks available, then assigns a small task
                            if totalLargeFileSize == 0:

                                task = tasks[i]

                                WAITING = False

                                break

                            # if there is a large task available, assigns it to the worker
                            elif float(tasks[i][7]) >= largeFileSize:

                                task = tasks[i]

                                WAITING = False

                                break

                        # runs if free worker isn't fast but there are existing fast workers.
                        elif queueid not in fastWorkers and fastQueue:

                            # if there is a small task available, assigns it
                            if float(tasks[i][7]) < largeFileSize:

                                task = tasks[i]

                                WAITING = False

                                break

                            # if there is a large task, then the following lines check to see if the database is overloaded or if there are no small tasks left
                            elif float(tasks[i][7]) >= largeFileSize:

                                # if there are only large tasks available and freeWorkerLargeFile is true (see beginning lines of function), assigns task to free worker
                                if (totalSmallFileSize == 0) and (freeWorkerLargeFile == True):

                                    task = tasks[i]

                                    WAITING = False

                                    break

                                # If there is an overload of large files, then assigns large file to worker
                                elif (totalLargeFileSize >= largeFileOverloadCutoff) and (largeFileOverloadCutoff != 0):

                                    task = tasks[i]

                                    WAITING = False

                                    break

                                # If there are no small tasks and freeWorkerLargeFile (see beginning lines of function) is false.
                                elif (totalSmallFileSize == 0) and not freeWorkerLargeFile:

                                    # If there is a free fast worker, then assigns the large task to fast worker
                                    # 'WAITING' while loops keeps repeating until either a fast worker becomes available or a small task is added to the database
                                    for v in fastWorkers:

                                        if workers[v][4] == 0:

                                            queue = workers[v][0]

                                            queueid = v

                                            task = tasks[i]

                                            WAITING = False

                                            break


                        # if there is no fast queue, then it doesn't matter what task is assigned to the worker
                        elif not fastQueue:

                            task = tasks[i]

                            WAITING = False

                            break
                except:

                    # Ignores errors that may arise if tasks list is empty and allows while loop to keep checking for new tasks
                    pass

            # If there are no tasks in the queue, but jobs are still being completed by other processes
            if (WAITING):

                # returns index of worker if worker completed a task, otherwise returns -1
                answr = checkTasks(numWorkers, workers)

                if answr != -1:

                    print (" ")
                    numTasks = numTasks-1

                    # If there are already 0 tasks left, makes sure numTasks doesn't become -1
                    if numTasks < 0:
                        numTasks = 0
                    print ("%d task(s) left in queue (task complete by process #%d)" % (numTasks,answr+1)) # lets user know task completed
                    workers[answr][4] = 0

                    cur.execute("DELETE FROM public.media WHERE id=%s" % (repr(workers[answr][5])))

                    axle.commit() # commits changes to database

                    workers[answr][5] = 0

                time.sleep(2) # Gives while loop 2 second break between iterations, reduces energy usage of program when waiting long periods of time for a new task.

        # The following lines gather information of task to be completed by worker and makes sure it is properly formatted

        NoError = True # assumes that there are no format errors

        Bitrate = False # assumes bitrate argument isn't included in the task

        startB = False # assumes that the user didn't give a start time

        durationB = False # assumes that the user didn't give a duration time


        bitR=[]

        start=''

        duration=''

        inP = task[0] #assigns input path

        outP = task[1] #assigns output path

        # if start time is given
        if(task[3] != None):

            start = task[3]

            startB = True

        # if duration time is given
        if(task[4] != None):

            duration = task[4]

            durationB = True

        # if bitrate is given
        if(task[2] != None):

            Bitrate = True

            bit = task[2]

            bitR.append(bit)

            if('/' in bit):

                bitR = task[2].split('/')

        # The following lines command the worker process to complete its assigned task using the provided task information

        if(Bitrate & startB & durationB & NoError):

            result = ffmpegVideo.apply_async(args=[inP, outP, bitR, start, duration], queue=queue)

        elif(startB & durationB & NoError):

            result = ffmpegVideo.apply_async(args=[inP, outP, start, duration], queue=queue)

        elif(Bitrate & startB & NoError):

            result = ffmpegVideo.apply_async(args=[inP, outP, bitR, start], queue=queue)

        elif(startB & NoError):

            result = ffmpegVideo.apply_async(args=[inP, outP, start], queue=queue)

        elif(Bitrate & NoError):

            result = ffmpegVideo.apply_async(args=[inP, outP, bitR], queue=queue)

        else:

            result = ffmpegVideo.apply_async(args=[inP, outP.rstrip()], queue=queue)

        # updates database to indicate task is in the process of being completed
        id = task[8]
        cur.execute("UPDATE public.media SET status = 'PENDING' WHERE id=%s" % (repr(id)))

        print ("\nProcess #%d: transcoding %s ...\n" % (queueid+1,outP))

        # updates worker list to indicate its busy with task
        workers[queueid][4] = result # result is a special object with a 'status' instance variable that can later be used to check if a task is completed
        workers[queueid][5] = id
        
        axle.commit() # commits changes to database

        
# Function returns an approximation (in MB) of the size of the transcode task
def getVideoSize(inP, outP, *args):

    Extra = args
    bitR = []
    start = 0
    duration = 0

    startTime = 0

    durationTime = 0

    try:
        if isinstance(Extra[0], list) and Extra[0] != ['']:
            bitR = Extra[0]
            try:
                bitR[1] = int(bitR[1])
                bitR[0] = int(bitR[0])

            except IndexError:
                bitR = int(bitR[0])

            try:
                start = Extra[1]

                startTime = start

                x = time.strptime(startTime.split(',')[0],'%H:%M:%S')

                startTime = float(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())
                try:
                    duration = Extra[2]

                    durationTime = duration

                    y = time.strptime(durationTime.split(',')[0],'%H:%M:%S')

                    durationTime = float(datetime.timedelta(hours=y.tm_hour,minutes=y.tm_min,seconds=y.tm_sec).total_seconds())
                except IndexError:
                    pass
            except IndexError:
                pass

        elif(isinstance(Extra[0], str)):
            start = Extra[0]

            startTime = start
            x = time.strptime(startTime.split(',')[0],'%H:%M:%S')

            startTime = float(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())
            try:
                duration = Extra[1]

                durationTime = duration
                y = time.strptime(durationTime.split(',')[0],'%H:%M:%S')

                durationTime = float(datetime.timedelta(hours=y.tm_hour,minutes=y.tm_min,seconds=y.tm_sec).total_seconds())
            except IndexError:
                pass

        totalDuration = subprocess.check_output("""/bin/bash -lc 'ffprobe -i "%s" -show_entries format=duration -v quiet -of csv="p=0"'""" % (inP), shell=True)
        totalDuration = totalDuration.rstrip()
        totalDuration = float(totalDuration)

        if(isinstance(Extra[0], str)):
            if (isinstance(start, str) & isinstance(duration, str)): #if start and duration are included

                percentOfVideo = durationTime/totalDuration

                videoSize = float(os.path.getsize(inP))/1048576*percentOfVideo

            elif(isinstance(start, str)): #if start is included

                percentOfVideo = (totalDuration-startTime)/totalDuration

                videoSize = float(os.path.getsize(inP))/1048576*percentOfVideo
        else:
            if isinstance(bitR, list) | isinstance(bitR, int) & isinstance(start, str) & isinstance(duration, str): #if bitrate, start, and duration are included

                percentOfVideo = durationTime/totalDuration

                videoSize = float(os.path.getsize(inP))/1048576*percentOfVideo
            elif (isinstance(bitR, list) | isinstance(bitR, int)) & isinstance(start, str): #if bitrate and start are included

                percentOfVideo = (totalDuration-startTime)/totalDuration

                videoSize = float(os.path.getsize(inP))/1048576*percentOfVideo

            elif (isinstance(bitR, list) | isinstance(bitR, int)): #if bitrate is included

                videoSize = float(os.path.getsize(inP))/1048576

    except:
        videoSize = float(os.path.getsize(inP))/1048576

    return videoSize


# Function analyzes info in a single task and uses it to call the function getVideoSize with the correct parameters
def initializeVideoSize(task):
    NoError = True #assumes that there are no format errors
    Bitrate = True #assumes bitrate argument is included in the task
    startB = False #assumes that the user didn't give a start time
    durationB = False #assumes that the user didn't give a duration time
    inP=''
    outP=''
    bitR=[]
    start=''
    duration=''
    if((task[0] == None) | (task[1] == None)): #checks for format errors in the text file
        print('\nFormat error in the following task: ' + str(task))
        print('Correct format: -i [Input Path] -o [Output Path] -b [Video Bitrate/Audio Bitrate (optional)] -s [hh:mm:ss (optional)] -d [hh:mm:ss (optional)]')
        NoError = False
    else: #if no format errors
        inP = task[0] #assigns input path
        outP = task[1] #assigns output path
    if(task[3] != None):
        start = task[3]
        startB = True
    if(task[4] != None):
        duration = task[4]
        durationB = True
    if(task[2] == None):
        Bitrate = False
    else:
        bit = task[2]
        bitR.append(bit)
        if('/' in bit):
            bitR = []
            bitR = task[2].split('/')
    try: #checks for format errors with the start/duration arguments
        if(((start[2] != ':') & (start[5] != ':')) | ((duration[2] != ':') & (duration[5] != ':'))):
            print('\nFormat error on line ' + str(int(i+1)))
            print('Correct format: -i [Input Path] -o [Output Path] -b [Video Bitrate/Audio Bitrate (optional)] -s [hh:mm:ss (optional)] -d [hh:mm:ss (optional)]')
            NoError = False
        elif((start[2] != ':') & (start[5] != ':')):
            print('\nFormat error on line ' + str(int(i+1)))
            print('Correct format: -i [Input Path] -o [Output Path] -b [Video Bitrate/Audio Bitrate (optional)] -s [hh:mm:ss (optional)] -d [hh:mm:ss (optional)]')
            NoError = False
    except:
        pass

    if(Bitrate & startB & durationB & NoError):
        videoSize = getVideoSize(inP, outP, bitR, start, duration)
    elif(startB & durationB & NoError):
        videoSize = getVideoSize(inP, outP, start, duration)
    elif(Bitrate & startB & NoError):
        videoSize = getVideoSize(inP, outP, bitR, start)
    elif(startB & NoError):
        videoSize = getVideoSize(inP, outP, start)
    elif(Bitrate & NoError):
        videoSize = getVideoSize(inP, outP, bitR)
    elif(NoError):
        videoSize = getVideoSize(inP, outP.rstrip())
    return videoSize


# Function checks to see if any process is free to start a new task and return's it's index in the workers list
def checkTasks (numWorkers, workers):
    answr = -1
    for w in range(numWorkers):
        if workers[w][4] == 0:
            pass
        elif(workers[w][4].status == 'SUCCESS'):
            answr = w
            break
    return answr


# Function returns the cumulative size (in MB) of the large tasks in the queue
def largeFileQueueSize (tasks, largeFileSize):
    totalLargeFileSize = 0
    for i in range(len(tasks)):
        if (float(tasks[i][7]) >= largeFileSize) and (tasks[i][5] == 'INCOMPLETE'):
            totalLargeFileSize += float(tasks[i][7])
    return float(totalLargeFileSize)


# Function returns the cumulative size (in MB) of the small tasks in the queue
def smallFileQueueSize (tasks, largeFileSize):
    totalSmallFileSize = 0
    for i in range(len(tasks)):
        if (float(tasks[i][7]) < largeFileSize) and (tasks[i][5] == 'INCOMPLETE'):
            totalSmallFileSize += float(tasks[i][7])
    return float(totalSmallFileSize)

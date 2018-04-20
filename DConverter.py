# DConverter.py
# Faustino Cortina


from celery import Celery
import subprocess

# Remote Setup for Celery
# If on a remote machine, change localhost to the IP address of the machine running RabbitMQ
app = Celery('tasks', backend='amqp', broker='amqp://axle:password@localhost')


# The following function can be run by Celery worker instances and transcodes an input file, inP, into an output file, outP
# using the open-source multimedia library FFMPEG

@app.task

def ffmpegVideo(inP, outP, *args):

    Extra = args

    Audio = False

    bitR = 0

    start = 0

    duration = 0


    try:

        if((isinstance(Extra[0], list)) & (Extra[0] != [''])):

            bitR = Extra[0]

            try:

                bitR[1] = int(bitR[1])

                bitR[0] = int(bitR[0])

            except IndexError:

                Audio = True

                bitR = int(bitR[0])

            try:

                start = str(Extra[1])

                try:

                    duration = str(Extra[2])

                except IndexError:

                    pass

            except IndexError:

                pass

        elif(isinstance(Extra[0], str) or isinstance(Extra[0], unicode)):

            start = str(Extra[0])

            try:

                duration = str(Extra[1])

            except IndexError:

                pass



        if(isinstance(Extra[0], str) or isinstance(Extra[0], unicode)):

            if(isinstance(start, str) & isinstance(duration, str)): #if start and duration are included

                subprocess.call('ffmpeg -i "%s" -ss %s -t %s -async 1 -y "%s"' % (inP, start, duration, outP), shell=True)

            elif(isinstance(start, str)): #if start is included

                subprocess.call('ffmpeg -i "%s" -ss %s -async 1 -y "%s"' % (inP, start, outP), shell=True)

        else:

            if((isinstance(bitR, list) | isinstance(bitR, int)) & isinstance(start, str) & isinstance(duration, str)): #if bitrate, start, and duration are included

                if(Audio): #only set audio bitrate

                    subprocess.call('ffmpeg -i "%s" -b:a %dk -ss %s -t %s -async 1 -y "%s"' % (inP, bitR, start, duration, outP), shell=True)

                elif(bitR != []):

                    subprocess.call('ffmpeg -i "%s" -b:v %dk -b:a %dk -ss %s -t %s -async 1 -y "%s"' % (inP, bitR[0], bitR[1], start, duration, outP), shell=True)

            elif((isinstance(bitR, list) | isinstance(bitR, int)) & isinstance(start, str)): #if bitrate and start are included

                if(Audio): #only set audio bitrate

                    subprocess.call('ffmpeg -i "%s" -b:a %dk -ss %s -async 1 -y "%s"' % (inP, bitR, start, outP), shell=True)

                elif(bitR != []):

                    subprocess.call('ffmpeg -i "%s" -b:v %dk -b:a %dk -ss %s -async 1 -y "%s"' % (inP, bitR[0], bitR[1], start, outP), shell=True)


            elif((isinstance(bitR, list) | isinstance(bitR, int))): #if bitrate is included

                if(Audio): #only set audio bitrate

                    subprocess.call('ffmpeg -i "%s" -b:a %dk -y "%s"' % (inP, bitR, outP), shell=True)

                elif(bitR != []):

                    subprocess.call('ffmpeg -i "%s" -b:v %dk -b:a %dk -y "%s"' % (inP, bitR[0], bitR[1], outP), shell=True)

    except:

        subprocess.call('ffmpeg -i "%s" -y "%s"' % (inP, outP), shell=True)

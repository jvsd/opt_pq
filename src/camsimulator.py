import os
import zmq
import glob
from PIL import Image
import _mysql
from cStringIO import StringIO

#Path to images
IMAGES = '/Users/jamesd/Dropbox/ARC Documentation/LAARK/images/keystone'

#Query Templates
IDLEFT = 'select id from images_left where file_name = '
IDRIGHT = 'select id from images_right where file_name = '
TELEMLEFT = 'select * from telemetry_left where id = '
TELEMRIGHT = 'select * from telemetry_right where id = '

#Will work locally, 150.135.158.187 should work remotely 
db = _mysql.connect('10.0.2.106','laark','laark4u','maryland')

cont = zmq.Context()
cam1 = cont.socket(zmq.REP)
cam2 = cont.socket(zmq.REP)

cam1.bind("tcp://*:6000")
cam2.bind("tcp://*:6001")

poller = zmq.Poller()
poller.register(cam1, zmq.POLLIN)
poller.register(cam2, zmq.POLLIN)



for file in glob.glob( os.path.join(IMAGES, '*.jpg') ):
    #Assuming that 'image.jpg' is the last item always
    split_file = file.split('/')
    img_name = split_file[len(split_file)-1]
    #if the image is from the left or right
    if img_name[0] == 'l':
        db.query(IDLEFT + "'" + img_name + "'")
    else:
        db.query(IDRIGHT + "'" + img_name + "'")
        
    result = db.store_result()
    id_temp = result.fetch_row()
    img_id = int(id_temp[0][0])
    
    if img_name[0] == 'l':
        db.query(TELEMLEFT + str(img_id))
    else:
        db.query(TELEMRIGHT + str(img_id))

    result = db.store_result()
    telem_temp = result.fetch_row()
#[id,time,rollVelo,pitchVelo,yawVelo,accelX,accelY,accelZ,tas,barAlt,oat,roll,pitch,yaw,gpsLat,gpsLon,gpsAlt,gpsVnorth,gpsVEast,gpsVDown,gpsYear,gpsMonth,gpsDat,gpsHour,gpsMinute,gpsSecond,gpsVisSats,gpsTrackSats,gpsPdop,ugpsStatus]
    img_telem = telem_temp[0]
    img = Image.open(file)
    img_buf = StringIO()
    img.save(buf,"JPEG",quality=100)

    socks = dict(poller.poll())
#LEFT/RIGHT images won't match to cam1/cam2 but that doesn't really matter
    if socks:
        if socks.get(cam1) == zmq.POLLIN:
            cam1.send(img_telem,zmq.SNDMORE)
            cam1.send(img_buf)
        if socks.get(cam2) == zmq.POLLIN:
            cam2.send(img_telem,zmq.SNDMORE)
            cam2.send(img_buf)

    print img_name
    

        


#!/usr/bin/env python
# coding=utf-8
import cv2
import requests

def getPicFeatureFromUrl(link):
    try:
        resp = requests.get(link)
        if resp.status_code == 200:
            suffix = link.rfind('.')
            if suffix < 0 or len(link)-suffix > 5:
                return 0, 0
            path = '/tmp/tmp%s'%link[suffix:]
            fp = open(path, 'w')
            fp.write(resp.content)
            fp.close()
            gray = cv2.imread(path, 0)
            sobel = cv2.Sobel(gray,cv2.CV_16U,1,1)
            clarity = cv2.mean(sobel)[0]
            bright = cv2.mean(gray)[0]
            return clarity*100, bright
    except:
        pass
    return 0, 0

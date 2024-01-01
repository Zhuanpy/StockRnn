import pyautogui
import time
import cv2
import numpy as np
time.sleep(5)
img = pyautogui.screenshot(region=[0, 0, 1366, 768])  # x,y,w,h
img.save('screenshot.png')

# img = cv2.cvtColor(np.asarray(img), cv2.COLOR_RGB2BGR)

import cv2
import pyautogui
import time
from code.FlightTicket.utils import MouseKeyBoard, key_inform


def screen_shot(name: str, path: str, size_=(0, 0, 1366, 768)):
    img = pyautogui.screenshot(region=[size_[0], size_[1], size_[2], size_[3]])  # x,y,w,h
    img.save(f'{path}/{name}')


def return_location(template: str, path_='GalilueTest'):

    target = 'screenshot.png'

    start_x, tempX = 0, 0
    start_y, tempY = 0, 0

    max_ = 0

    while max_ < 0.99:
        screen_shot(name=target, path=path_)
        target_ = f"{path_}/{target}"
        template_ = f"{path_}/{template}"

        tar = cv2.imread(target_)
        temp = cv2.imread(template_)

        res = cv2.matchTemplate(tar, temp, cv2.TM_CCORR_NORMED)

        min_, max_, min_loc, max_loc = cv2.minMaxLoc(res)

        if max_ < 0.99:
            time.sleep(3)

        tempX = temp.shape[1]
        tempY = temp.shape[0]

        start_x, start_y = max_loc

    center_x = int(start_x + tempX / 2)
    center_y = int(start_y + tempY / 2)

    loc_ = (center_x, center_y)

    return loc_


mk = MouseKeyBoard()

x, y = return_location('SmallPycharm.jpg')  # 提取PYCHARM 窗口位置
mk.click_page(x, y)  # 最小化 PYCHARM 窗口

""" 双击 smart logo"""
x, y = return_location('TravelPortLog.jpg')  # 获取 Smart logo 位置
mk.click_page(x, y)  #

""" login board """
return_location('LoginPage.png')  # 获取 Smart logo 位置

""" 输入账号"""
x, y = return_location('LoginName.jpg')  # 获取 Smart logo 位置
mk.click_page(x, y)  #
name_ = 'ZL5'
key_inform(name_)
time.sleep(1)

""" 输入密码 """
pass_ = 'NAUG2022ZZ'
x, y = return_location('LoginPassWord.jpg')  # 获取 Smart logo 位置
mk.click_page(x, y)  #
key_inform(pass_)

import datetime

def printdt():
    dt_now = datetime.datetime.now()

    dt_str = dt_now.strftime("%Y-%m-%d %H:%M:%S")

    print("Hello World with Mizuno_" + dt_str)

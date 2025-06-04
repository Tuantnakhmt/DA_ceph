import colorama
import datetime
def formatDate(x):
    '''
    Chuyển đổi thời gian từ dd/mm/yyyy sang yyyy-mm-dd'''
    s = x.split("/")
    return f"{s[2]}-{s[1]}-{s[0]}"

def CoverTime(time, year = True):
    '''
    Chuyển đổi thời gian từ dd/mm/yyyy sang khoảng thời gian cố định đầu tư
    '''
    if year==False:
        start,end = datetime.datetime(2000+(time)//4,(time%4)*3+2,6),datetime.datetime(2000+(time)//4 +((time%4)*3+5)//12,((time%4)*3+5)%12,1)
    else:
        start,end = datetime.datetime(time,4,1),datetime.datetime(time+1,3,1)
    return start,end

def coverTime(str_time):
    '''
    Chuyển đổi thời gian từ dd/mm/yyyy sang yyyy-mm-dd
    '''
    time = str_time.split("/")
    return datetime.datetime(int(time[2]),int(time[1]),int(time[0]))

def progress_bar(cur,total,color=colorama.Fore.GREEN,text=""):
    '''
    Hiển thị thanh tiến trình'''
    percent = 100*(cur/float(total))*1/2
    bar = '█' * int(percent) + "-"*(50-int(percent))
    print(colorama.Fore.BLUE + f"\r |{bar}| {percent*2:.2f}% {text}",end="\r")
    if cur == total:
        print(colorama.Fore.RESET + f"|{text} Done!!!!| {percent*2:.2f}%")
from datetime import datetime as dt
import datetime



def printSinceUntilToday(since):

    since = dt.strptime(since, '%Y-%m-%d')
    today = dt.today()

    rtrlist = []

    for x in range(0,int(str(today-since).split()[0])):
        rtrlist.append(str((since+ datetime.timedelta(days=x)))[:10])

    return rtrlist

def todayStr(type):
    if(type=='-'):
            return str(dt.today())[:10]
    else:
            return str(dt.today())[:10].replace('-','')


if __name__ == '__main__':

    print('wow')
    print(todayStr('-'))
    a = (1,2,3)
    b = (4,5,6)

    print(a+b)
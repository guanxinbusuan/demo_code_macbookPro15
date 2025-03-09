#练习四种断点调试方法

#1. 条件断点（Expression）
#2. 命中次数断点（Hit Count）
#3. 日志断点（Log Message）
#4. 联动断点 （Wait for Breakpoint）
def multiply(x, y):
    result = x * y
    return result

if __name__ == '__main__':
    demo_list = [(1,2),(3,4),(5,-6)]
    for a,b in demo_list:
        print(f"{a}*{b}={multiply(a,b)}")
    


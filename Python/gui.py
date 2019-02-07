import tkinter
import tkinter.ttk
a=1
window=tkinter.Tk()

window.title("대용량 빅데이터 전처리 시스템 ver0.1")
window.geometry("720x720+300+100")
window.resizable(True, False)

label_main=tkinter.Label(window, text="메인", width=10000, height=3,relief="ridge",bd=10)
label_main.pack()

label=tkinter.Label(window, text="count", width=10000, height=3,relief="ridge",bd=10)
label.pack()
count=0

def countUP():
    global count
    count +=1
    label.config(text="count="+str(count))


button = tkinter.Button(window,text="click=countUP", overrelief="ridge", width=15, command=countUP, repeatdelay=1000)
button.pack()

b4=tkinter.Button(window, text="b4(0, 200)")
b5=tkinter.Button(window, text="b5(0, 300)")
b6=tkinter.Button(window, text="b6(0, 300)")

b4.place(x=0, y=200, relwidth=0.5)
b5.place(x=0, y=200, relx=0.5)
b6.place(x=0, y=250, relx=0.5)

def calc(event):
    label.config(text="결과="+str(eval(entry.get())))

entry=tkinter.Entry(window)
entry.bind("<Return>", calc)
entry.pack()

hello=tkinter.Label(window)
hello.pack()


progressbar=tkinter.ttk.Progressbar(window, maximum=100, mode="determinate")
progressbar.pack()
progressbar["maximum"] = 100
progressbar["value"] = 30
progressbar.start(100)
window.mainloop()

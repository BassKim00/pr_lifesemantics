import tkinter.ttk
#import sql_connect as sqc
from tkinter import Label

window = tkinter.Tk()


window.title("대용량 빅데이터 전처리 시스템 ver0.1")
window.geometry("720x720+300+100")
window.resizable(True, False)

label_main = tkinter.Label(window, text="메인", relief="ridge", bd=5)
label_main.pack(side="top",fill="x")

label = tkinter.Label(window, text="count", relief="ridge", bd=5)
label.pack(side="top",fill="x")


# frame

frame1=tkinter.Frame(window, relief="solid", bd=2)
frame1.pack(side="left", fill="both", expand=True)

frame2=tkinter.Frame(window, relief="solid", bd=2)
frame2.pack(side="right", fill="both", expand=True)
# button

count = 0
def countUP():
    global count
    count += 1
    label.config(text="count=" + str(count))


button = tkinter.Button(window, text="click=countUP", overrelief="ridge", width=15, command=countUP, repeatdelay=1000)
button.pack(side="top")

b4 = tkinter.Button(frame1, text="b4(0, 200)")
b5 = tkinter.Button(frame2, text="b5(0, 300)")
b6 = tkinter.Button(window, text="b6(0, 300)")

b4.place()
b5.place(x=0, y=200, relx=0.5)
b6.place(x=0, y=250, relx=0.5)
b4.pack(side="top",anchor="w")
b5.pack(side="top",anchor="e")
b6.pack(side="top",fill="x")


def calc(event):
    hello.config(text="결과=" + str(eval(entry.get())))

# entry

entry = tkinter.Entry(frame1)
entry.bind("<Return>", calc)
entry.pack(side="left",anchor="n")

hello = tkinter.Label(frame2,text="!!!!!!!!!!!!", relief="ridge", bd=5)
hello.pack(side="right",anchor="n")

# list box

listbox = tkinter.Listbox(window, selectmode='extended', height=0)
listbox.insert(0, "DB_HOST")
listbox.insert(1, "DB_USER")
listbox.insert(2, "DB_PASSWORD")
listbox.insert(3, "DB_NAME")
listbox.pack(side="top")

# menu
def close():
    window.quit()
    window.destroy()

menubar=tkinter.Menu(window)

menu_1=tkinter.Menu(menubar, tearoff=0)
menu_1.add_command(label="하위 메뉴 1-1")
menu_1.add_command(label="하위 메뉴 1-2")
menu_1.add_separator()
menu_1.add_command(label="하위 메뉴 1-3", command=close)
menubar.add_cascade(label="상위 메뉴 1", menu=menu_1)

menu_2=tkinter.Menu(menubar, tearoff=0, selectcolor="red")
menu_2.add_radiobutton(label="하위 메뉴 2-1", state="disable")
menu_2.add_radiobutton(label="하위 메뉴 2-2")
menu_2.add_radiobutton(label="하위 메뉴 2-3")
menubar.add_cascade(label="상위 메뉴 2", menu=menu_2)

menu_3=tkinter.Menu(menubar, tearoff=0)
menu_3.add_checkbutton(label="하위 메뉴 3-1")
menu_3.add_checkbutton(label="하위 메뉴 3-2")
menubar.add_cascade(label="상위 메뉴 3", menu=menu_3)

window.config(menu=menubar)

# menu button
menubutton=tkinter.Menubutton(window,text="메뉴 메뉴버튼", relief="raised", direction="right")
menubutton.pack()

menu=tkinter.Menu(menubutton, tearoff=0)
menu.add_command(label="하위메뉴-1")
menu.add_separator()
menu.add_command(label="하위메뉴-2")
menu.add_command(label="하위메뉴-3")

menubutton["menu"]=menu

# message

message=tkinter.Message(window, text="메세지입니다.", width=100, relief="solid")
message.pack()

# progress bar
progressbar = tkinter.ttk.Progressbar(window, maximum=1000, mode="determinate")
progressbar.pack()
progressbar["maximum"] = 100
progressbar["value"] = 30
progressbar.start(100)
window.mainloop()

print("Window Close")
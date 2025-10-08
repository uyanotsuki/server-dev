import threading
import time
import random


class Pot:
    def __init__(self, pot_size, num_savages=0):
        self.pot_size = pot_size  # вместимость
        self.num_savages = num_savages
        self.portions = 0 # текущ кол-во порций
        self.next_savage = 0
        self.lock = threading.Lock() # основной мьютекс
        self.output_lock = threading.Lock() # мьютекс для вывода
        self.empty = threading.Condition(self.lock) # условие пусто
        self.full = threading.Condition(self.lock) # условие полно

    def print_sync(self, message):
        with self.output_lock:
            print(message)

    # без чередования
    def take_portion(self, savage_id):
        with self.lock:
            while self.portions == 0:
                self.print_sync(f"Дикарь {savage_id} ждет, кастрюля пуста")
                self.empty.notify() # будим повара
                self.full.wait()    # ждем пока наполнят
            self.portions -= 1
            self.print_sync(f"Дикарь {savage_id} взял порцию. Осталось: {self.portions}")
            return True

    # по очереди
    def take_portion_fair(self, savage_id):
        with self.lock:
            while savage_id != self.next_savage:  # ждет своей очереди
                self.full.wait()

            while self.portions == 0:
                self.print_sync(f"Дикарь {savage_id} будит повара")
                self.empty.notify()
                self.full.wait()

            self.portions -= 1
            self.print_sync(f"Дикарь {savage_id} взял порцию. Осталось: {self.portions}")

            self.next_savage = (self.next_savage + 1) % self.num_savages # циклическая очередь
            self.full.notify_all()  # будим всех
            return True

    def fill_pot(self):
        with self.lock:
            while self.portions > 0:  # ждет пока кастрюля опустеет
                self.print_sync("Повар ждет, кастрюля еще не пуста")
                self.empty.wait()   # ждет сигнала пусто
            self.portions = self.pot_size  # наполняет кастрюлю
            self.print_sync(f"Повар наполнил кастрюлю. Порций: {self.portions}")
            self.full.notify_all()

# нет порядка кто за кем
def savage(savage_id, pot):
    time.sleep(random.uniform(0.1, 0.5))
    pot.take_portion(savage_id)  # взял порцию
    pot.print_sync(f"Дикарь {savage_id} поел")

# едят по очереди
def hungry_savage(savage_id, pot, meals_to_eat=1):
    for meal in range(meals_to_eat):
        time.sleep(random.uniform(0.1, 0.3))
        pot.take_portion_fair(savage_id)
        pot.print_sync(f"Дикарь {savage_id} съел порцию {meal + 1}")
        time.sleep(random.uniform(0.1, 0.2))


def cook(pot, num_savages):
    for _ in range(num_savages // pot.pot_size + 1):
        pot.fill_pot()


def continuous_cook(pot):
    while True:
        pot.fill_pot()
        time.sleep(0.1)


def stolovka():
    print("== Без чередования ==")
    z = 4
    x = 6

    pot = Pot(z)

    savage_threads = []
    for i in range(x):
        thread = threading.Thread(target=savage, args=(i, pot))
        savage_threads.append(thread)
        thread.start()

    cook_thread = threading.Thread(target=cook, args=(pot, x))
    cook_thread.start()

    for thread in savage_threads:
        thread.join()
    cook_thread.join()

    print("Все дикари поели!\n")


def stolovka_s_ochered():
    print("== С чередованием ==")
    z = 4
    x = 6

    pot = Pot(z, x)

    cook_thread = threading.Thread(target=continuous_cook, args=(pot,))
    cook_thread.daemon = True
    cook_thread.start()

    savage_threads = []
    for i in range(x):
        thread = threading.Thread(target=hungry_savage, args=(i, pot, 1))
        savage_threads.append(thread)
        thread.start()

    for thread in savage_threads:
        thread.join()

    print("Все дикари наелись!\n")


def main():
    print('Обед дикарей')

    stolovka()
    time.sleep(1)
    stolovka_s_ochered()

if __name__ == "__main__":
    main()
#  Домашние электроприборы. Определить иерархию электроприборов. Включить некоторые в розетку. 
# Подсчитать потребляемую мощность. Провести сортировку приборов в квартире на основе мощности. 
# Найти прибор в квартире, соответствующий заданному диапазону параметров

# класс для приборов
class ElectricalAppliance:
    def __init__(self, name, power, is_plugged=False):
        self.name = name
        self.power = power
        self.is_plugged = is_plugged
    
    def __str__(self):
        status = "включен" if self.is_plugged else "выключен"
        return f"{self.name}: {self.power} Вт, статус: {status}"

# для управления 
class Home:
    def __init__(self):
        self.appliances = []
    
    def add_appliance(self, appliance):
        self.appliances.append(appliance)
    
    def show_all(self):
        if not self.appliances:
            print("Нет приборов")
            return
        
        for i, appliance in enumerate(self.appliances, 1):
            print(f"{i}. {appliance}")
    
    def toggle_power(self, index):
        if 0 <= index < len(self.appliances):
            # статус на противоположный
            self.appliances[index].is_plugged = not self.appliances[index].is_plugged
            # перезапись для консоли
            status = "включен" if self.appliances[index].is_plugged else "выключен"
            print(f"{self.appliances[index].name} теперь {status}")
        else:
            print("Неверный номер прибора")
    
    def calculate_total_power(self):
        total = sum(appliance.power for appliance in self.appliances if appliance.is_plugged)
        print(f"Общая потребляемая мощность: {total} Вт")
        return total
    
    def sort_by_power(self):
        self.appliances.sort(key=lambda x: x.power, reverse=True)
        print("Приборы отсортированы по мощности")
    
    def find_by_power_range(self, min_power, max_power):
        result = [appliance for appliance in self.appliances 
                 if min_power <= appliance.power <= max_power]
        
        if not result:
            print("Приборы в указанном диапазоне не найдены")
            return
        
        print(f"Приборы мощностью {min_power}-{max_power} Вт:")
        for appliance in result:
            print(f"  {appliance}")


def main():
    home = Home()

    home.add_appliance(ElectricalAppliance("Холодильник", 150, True))
    home.add_appliance(ElectricalAppliance("Телевизор", 120))
    home.add_appliance(ElectricalAppliance("Стиральная машина", 2100))
    home.add_appliance(ElectricalAppliance("Микроволновка", 800))
    home.add_appliance(ElectricalAppliance("Компьютер", 300))
    home.add_appliance(ElectricalAppliance("Электрочайник", 1800))
    home.add_appliance(ElectricalAppliance("Фен", 1600))
    home.add_appliance(ElectricalAppliance("Кондиционер", 2000))
    
    while True:
        print("\n=== Управление электроприборами ===")
        print("1. Показать все приборы")
        print("2. Включить/выключить прибор")
        print("3. Посчитать общую мощность")
        print("4. Отсортировать по мощности")
        print("5. Найти по диапазону мощности")
        print("0. Выход")
        
        choice = input("Выберите действие: ")
        
        if choice == '1':
            print("\nВсе приборы:")
            home.show_all()
            
        elif choice == '2':
            home.show_all()
            try:
                index = int(input("Номер прибора: ")) - 1
                home.toggle_power(index)
            except ValueError:
                print("Неверный ввод")
                
        elif choice == '3':
            home.calculate_total_power()
            
        elif choice == '4':
            home.sort_by_power()
            home.show_all()
            
        elif choice == '5':
            try:
                min_p = float(input("Минимальная мощность: "))
                max_p = float(input("Максимальная мощность: "))
                home.find_by_power_range(min_p, max_p)
            except ValueError:
                print("Неверный ввод мощности")
                
        elif choice == '0':
            print("Выход...")
            break
            
        else:
            print("Неверный выбор")


if __name__ == "__main__":
    main()
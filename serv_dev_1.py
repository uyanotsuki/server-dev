class Patient:

    def __init__(self, id, last_name, first_name, middle_name, address, phone, medical_card_number, diagnosis):
        self.id = id
        self.last_name = last_name
        self.first_name = first_name
        self.middle_name = middle_name
        self.address = address
        self.phone = phone
        self.medical_card_number = medical_card_number
        self.diagnosis = diagnosis

    def __str__(self):
        return (f"ID: {self.id}, ФИО: {self.last_name} {self.first_name} {self.middle_name}, "
                f"Номер мед. карты: {self.medical_card_number}, Диагноз: {self.diagnosis}")

class Hospital:

    def __init__(self):
        self.patients = []

    def add_patient(self, patient):
        self.patients.append(patient)

    def print_patients_by_diagnosis(self, diagnosis):
        print(f"\nСписок пациентов с диагнозом '{diagnosis}':")
        found = False
        for patient in self.patients:
            if patient.diagnosis == diagnosis:
                print(patient)
                found = True
        if not found:
            print("Пациенты с таким диагнозом не найдены.")

    def print_patients_by_medical_card_range(self, start, end):
        print(f"\nСписок пациентов с номером мед. карты в интервале от {start} до {end}:")
        found = False
        for patient in self.patients:
            if start <= patient.medical_card_number <= end:
                print(patient)
                found = True
        if not found:
            print("Пациенты в указанном диапазоне не найдены.")
 
    def get_patient_count(self):
        return len(self.patients)


if __name__ == "__main__":
    hospital = Hospital()
    
    hospital.add_patient(Patient(1, "Иванов", "Иван", "Иванович", "ул. Ленина, 1", "+79991234567", 1001, "Грипп"))
    hospital.add_patient(Patient(2, "Петров", "Петр", "Петрович", "ул. Пушкина, 5", "+79997654321", 1005, "Ангина"))
    hospital.add_patient(Patient(3, "Сидорова", "Мария", "Ивановна", "пр. Мира, 10", "+79995556677", 1003, "Грипп"))
    
    #по диагнозу
    hospital.print_patients_by_diagnosis("Грипп")
    
    #по диапазону номеров мед. карт
    hospital.print_patients_by_medical_card_range(1002, 1004)
    
    print(f"\nВсего пациентов: {hospital.get_patient_count()}")

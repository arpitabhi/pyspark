import random
import datetime
import os
import names

no_of_folders = 10000
no_of_files = 4
no_of_record = 1
parent_folder_path = os.getcwd()
gender = ['male','female']
department_code_list = ['aws_1','aws_2','aws_3','aws_4','aws_5']
start_dt = datetime.date.today().replace(day=1, month=1).toordinal()
end_dt = datetime.date.today().toordinal()


for folder in range(no_of_folders):
    folder_name = 'emp_' + str(folder)
    path = os.path.join(parent_folder_path,folder_name)
    os.mkdir(path)

    for file in range(0,no_of_files,2):
        file_with_schema_name = 'emp_' + str(file) + '_with_schema.csv'
        file_with_schema_path = os.path.join(path,file_with_schema_name)

        file_without_schema_name = 'emp_' + str(file+1) + '_without_schema.csv'
        file_without_schema_path = os.path.join(path,file_without_schema_name)

        file_schema_content = 'emp_no,first_name,last_name,age,sal,department_code,date_of_joining\n'
        file_without_schema_content = ''

        for record in range(no_of_record):
            emp_no = str(random.randint(100000,999999))
            first_name = names.get_first_name(gender=random.choice(gender))
            last_name = names.get_last_name()
            age = str(random.randint(18,70))
            sal = str(random.randint(100000,5000000))
            department_code = random.choice(department_code_list)
            date_of_joining =str(datetime.date.fromordinal(random.randint(start_dt, end_dt)))

            emp_no_1 = str(random.randint(100000,999999))
            first_name_1 = names.get_first_name(gender=random.choice(gender))
            last_name_1 = names.get_last_name()
            age_1 = str(random.randint(18,70))
            sal_1 = str(random.randint(100000,5000000))
            department_code_1 = random.choice(department_code_list)
            date_of_joining_1 =str(datetime.date.fromordinal(random.randint(start_dt, end_dt)))

            file_schema_content+= "{},{},{},{},{},{},{}\n".format(emp_no,first_name,last_name,age,sal,department_code,date_of_joining)
            file_without_schema_content+= f"{emp_no_1},{first_name_1},{last_name_1},{age_1},{sal_1},{department_code_1},{date_of_joining_1}\n"


        with open(file_with_schema_path,'w') as f:
            f.write(file_schema_content[:len(file_schema_content)-2])
        
        with open(file_without_schema_path,'w') as f:
            f.write(file_without_schema_content[:len(file_without_schema_content)-2])

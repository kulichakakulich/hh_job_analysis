import json
from pathlib import Path
import pandas as pd
import sqlalchemy
import re
from additional.personal_data import username, password, db_name, db_host, db_port, vacancies_folder


def load_vacancy_json(file_path: str):
    with open(file_path, encoding='utf8') as f:
        json_text = f.read()
    json_dict = json.loads(json_text)

    row = re.sub(r'<.*?>|[^\w ]', ' ', str(json_dict['description']).lower())
    vacancy = {
        'id': int(json_dict['id']),
        'name': json_dict['name'],
        'experience': json_dict['experience']['name'],
        'description': row,
        'company_id': int(json_dict['employer']['id']),
    }

    company = {
        'company_id': int(json_dict['employer']['id']),
        'name': json_dict['employer']['name']
    }

    salary = json_dict['salary']
    if salary is not None:
        vacancy['salary_from'] = salary['from']
        vacancy['salary_to'] = salary['to']
        vacancy['salary_currency'] = salary['currency']
    else:
        vacancy['salary_from'] = None
        vacancy['salary_to'] = None
        vacancy['salary_currency'] = None

    skills = [{'name': skill['name'], 'vacancy_id': json_dict['id']}
              for skill in json_dict['key_skills']]

    return vacancy, company, skills


def connect_database():

    vacancy_path = Path(vacancies_folder)
    vacancy_files = vacancy_path.glob('*.json')

    vacancies = []
    skills = []
    companies = []
    for file in vacancy_files:
        vacancy, company, skill = load_vacancy_json(file)
        vacancies.append(vacancy)
        companies.append(company)
        skills.extend(skill)

    df_vacancy = pd.DataFrame(vacancies)
    df_company = pd.DataFrame(companies).drop_duplicates()
    df_skill = pd.DataFrame(skills)
    print('Data frames created')

    engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{username}:{password}@{db_host}:{db_port}/{db_name}')
    with engine.connect() as conn:
        transaction = conn.begin()
        print('Database connection established')
        try:
            df_vacancy.to_sql('vacancy', engine, if_exists='replace', index=False)
            df_company.to_sql('company', engine, if_exists='replace', index=False)
            df_skill.to_sql('skill', engine, if_exists='replace', index=False)
            transaction.commit()
        except Exception as E:
            print(E)
            transaction.rollback()

        print('Data loaded into database')

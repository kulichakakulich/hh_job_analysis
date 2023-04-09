import json
from pathlib import Path
import pandas as pd
import sqlalchemy


def load_vacancy_json(file_path):
    with open(file_path, encoding='utf8') as f:
        json_text = f.read()
    json_dict = json.loads(json_text)

    vacancy = {
        'id': json_dict['id'],
        'name': json_dict['name'],
        'experience': json_dict['experience']['name'],
        'description': json_dict['description'],
        'company_id': json_dict['employer']['id'],
    }

    company = {
        'company_id': json_dict['employer']['id'],
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


if __name__ == '__main__':
    vacancy_path = Path('./docs/vacancies')
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

    username = 'postgres'
    password = '********'
    host = 'localhost'
    port = 5432
    database = 'data_hh'

    engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
    with engine.connect() as connection:
        print('Database connection established')
        df_vacancy.to_sql('vacancy', engine, if_exists='replace', index=False)
        df_company.to_sql('company', engine, if_exists='replace', index=False)
        df_skill.to_sql('skill', engine, if_exists='replace', index=False)
        print('Data loaded into database')

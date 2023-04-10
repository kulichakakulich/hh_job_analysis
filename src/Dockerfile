FROM python:3.9

WORKDIR /app

COPY requirements.txt /app/
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY ./scrape_vacancies.py /app/
COPY connect_bd.py /app/
COPY ./additional/gen_bd.py /app/additional/gen_bd.py
COPY ./additional/personal_data.py /app/additional/personal_data.py

CMD ["python", "scrape_vacancies.py"]

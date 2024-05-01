FROM python:3.11

LABEL authors="andrew"

WORKDIR /

COPY requirements.txt .


RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
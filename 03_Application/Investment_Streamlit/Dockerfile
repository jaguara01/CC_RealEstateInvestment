# lightweight Python image
FROM python:3.10-slim

# working directory in container
WORKDIR /app

# code to container
COPY . .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# expose port
EXPOSE 8501

# set streamlit to run on all interfaces
CMD ["streamlit", "run", "app.py", "--server.enableCORS=false", "--server.enableXsrfProtection=false", "--server.port=8501", "--server.address=0.0.0.0"]


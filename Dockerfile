# Use Python 3.10 base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Install dependencies for ChromeDriver and Chrome
RUN apt-get update && apt-get install -y \
    libnss3 \
    libgdk-pixbuf2.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxrandr2 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libxss1 \
    libnspr4 \
    libxtst6 \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libgdk-pixbuf2.0-0 \
    lsb-release \
    wget \
    curl \
    unzip \
    && apt-get clean

RUN apt-get update &&  apt-get install -y \
    libxss1 \
    libappindicator3-1 \
    fonts-liberation \
    libnss3 \
    xdg-utils \
    libgtk-3-0 \
    libgbm1 \
    libasound2 \
    libx11-xcb1 \
    libu2f-udev

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && apt-get clean

# Copy application files into the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirement.txt boto3

# Download and install Chrome from the specified URL
RUN wget https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chrome-linux64.zip -O /tmp/chrome-linux64.zip \
    && unzip /tmp/chrome-linux64.zip -d /opt/ \
    && rm /tmp/chrome-linux64.zip

# Create a symbolic link to the chrome binary
RUN ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome

# RUN wget https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.69/linux64/chrome-linux64.zip -O /tmp/chrome-linux64.zip \
#     && unzip /tmp/chrome-linux64.zip -d /usr/local/bin/ \
#     && rm /tmp/chrome-linux64.zip

# Download and unzip ChromeDriver
RUN wget https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chromedriver-linux64.zip -O /tmp/chromedriver.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf chromedriver-linux64 chromedriver-linux64.zip

RUN chmod +x /usr/bin/google-chrome
RUN chmod +x /usr/local/bin/chromedriver

# Expose the necessary ports for Selenium (if using headless browser)
EXPOSE 4444

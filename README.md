# InmoDecision: A Data-Driven Advisor for Real Estate Investment with Social Sentiment Analysis

This project showcases the design and implementation of a complete, end-to-end Big Data pipeline on a modern cloud architecture. The primary goal was to build a production-ready system that handles the entire data lifecycle: from real-time and batch ingestion using Apache Kafka and Python, through distributed processing with Apache Spark, to final consumption in an AWS Redshift data warehouse. The result is a sophisticated, scalable solution that transforms raw, diverse data into actionable insights via a user-facing Streamlit application.

![Tech](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/tech.png)

## 1. Project Overview and Goals

### The Challenge: Navigating the Modern Real Estate Market
In today’s volatile real estate market—particularly in major European cities like **Barcelona**—making informed investment or purchasing decisions is more challenging than ever.  

Buyers and investors face fragmented data from multiple sources, making it difficult to:
- Gauge a property’s **true value**
- Understand **market trends**
- Anticipate **future opportunities**

The missing piece is a unified platform that contextualizes **quantitative data** (property prices, specifications) with **qualitative insights** (public sentiment, neighborhood buzz).

### The Solution: InmoDecision
**InmoDecision** is an **end-to-end Big Data application** that acts as a decision-making platform for the Barcelona real estate market.  

It goes beyond simple listings by deploying a **cloud-native system** that ingests, processes, and analyzes large volumes of diverse data in real time.

Users gain:
- A **multi-faceted view** of properties  
- The ability to **spot undervalued assets**  
- Tools to **analyze profitability**  
- Insights from **social sentiment analysis**  

### Project Goals and Objectives
The project demonstrates expertise in **data engineering, cloud computing, and data science**.

**Key objectives:**
1. **Scalable Big Data Architecture**  
   - AWS pipeline with **Landing Zone**, **Trusted Zone**, **Exploitation Zone**

2. **Real-Time Streaming & Processing**  
   - Ingest/analyze data from **Bluesky** & **Apache Kafka**

3. **Predictive Machine Learning Model**  
   - Property price prediction, integrated into applications

4. **User-Facing Application**  
   - **Streamlit dashboard** for exploration and investment scenarios

5. **Cloud-Native & DevOps Best Practices**  
   - Serverless-first (AWS Fargate, Redshift Serverless), Docker, CloudFormation, GitHub Actions

![Architecture](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/BDM%20Arquitecture.jpg)


---

## 2. Data Consumption and Applications

### 2.1. Streamlit Web Application – Investment Analyzer
An interactive **Streamlit app** for user-friendly property exploration:

- **Price Predictor**  
  Predicts fair market value from property features  

- **Investment Plan**  
  Projects future cash flows & profitability

  ![Projection](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/Slack2.png)

- **Listings Explorer**  
  Map of Barcelona with property filters (price, rooms, bathrooms, etc.)
  
![Listing](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/Listing.png)

- **Sentiment Analysis**  
  Analyzes **Bluesky posts** for market sentiment  

![Sentiment](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/sentiment.png)

---

### 2.2. The Live Pulse: Real-Time Alerting System
Specialized app for **instant property alerts**:

- **Real-time Ingestion**  
  Property listings streamed into **Kafka**  

- **Real-time Prediction**  
  Python consumer fetches model from S3, compares price vs. predicted value  

- **Slack Notifications**  
  Alerts when undervalued properties appear  

![Slack](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/Slack1.png)
![Slack_Detail](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/Slack2.png)

---

## 3. Data Architecture: From Raw Data to Actionable Insights

*(Image: Project Data Flow Diagram from Landing Zone to Exploitation Zone)*

### 3.1. Landing Zone (Amazon S3)
- Entry point for **raw data** from external sources  
- Stored in S3 buckets (structured, semi-structured, unstructured)  

### 3.2. Trusted Zone (Amazon S3 + Apache Spark)
- **Data cleaning & validation** with Spark  
- Stored in **Parquet** format for efficient queries  

**Transformations:**
- **Structured:** schema correction, type casting, categorical normalization, integration  
- **Semi-Structured:** flatten JSON, extract coordinates, normalize text  
- **Unstructured:** anonymize PII, sanitize content, extract metadata  

### 3.3. Exploitation Zone (Amazon Redshift)
- Final **analytics-ready** layer in **Redshift Serverless**  
- Subject-based schemas (data mart style)  
- ETL ensures optimized, reliable data for applications  

---

## 4. Data Sources and Ingestion

**Government Open Data**  
- Sources: INE, Open Data Barcelona, ECB Data Portal  
- Data: prices, demographics, economic indicators, district amenities  
- Ingestion: Batch via Python scripts → S3  

**Property Listings Data**  
- Source: Idealista (simulated)  
- Data: live property listings (price, attributes, location)  
- Ingestion: Synthetic generator → Kafka → S3  

**Social Media Sentiment Data**  
- Source: Bluesky API  
- Data: real-time posts, text, metadata  
- Ingestion: Python WebSocket client → AWS Fargate → S3  

**Geospatial & POI Data**  
- Source: Google Places API  
- Data: JSON with amenities & businesses  
- Ingestion: API fetch → structured & stored  

---

## 5. Cloud Infrastructure and Deployment

- **Containerization:** Docker  
- **Compute:** AWS Fargate & ECS  
- **Frontend Deployment:** AWS Elastic Beanstalk (Streamlit app)  
- **CI/CD:** GitHub Actions + Amazon ECR  
- **Networking:** Amazon VPC with NAT Gateway  
- **Monitoring & Security:** CloudWatch + IAM policies   

---

## 6. Conclusions and Future Work

### Conclusion
The InmoDecision project successfully demonstrates the design, implementation, and deployment of a comprehensive, cloud-native Big Data application. By building a sophisticated, multi-layered data architecture on AWS, this project proves it is possible to transform vast amounts of raw, disparate data into a cohesive and valuable tool for real estate investment analysis. The platform effectively integrates historical market data, real-time social media sentiment, and predictive machine learning to deliver actionable insights through an intuitive web application and a real-time alerting system. This project not only meets its initial objectives but also serves as a robust, scalable foundation for a production-grade data product.

---

### Key Achievements
- **Successfully Engineered a Scalable, Cloud-Native Platform**  
  A complete, end-to-end data intelligence platform was built from the ground up, leveraging a serverless-first approach with AWS Fargate, S3, and Redshift Serverless. The architecture is designed for scalability, security, and maintainability, following modern DevOps and Twelve-Factor App principles.

- **Integrated Diverse Data Sources with Machine Learning**  
  The system effectively ingests and processes a wide variety of data—including structured government statistics, semi-structured geospatial data, and unstructured real-time social media text. This data is used to train and deploy a machine learning model for accurate property price prediction.

- **Delivered Actionable Insights via Dashboards and Real-Time Alerts**  
  The project culminates in tangible, user-facing applications that deliver real value. The Streamlit dashboard provides a rich, interactive environment for investment analysis, while the Slack-integrated alerting system offers immediate notifications on undervalued properties, turning data into timely opportunities.

---

### Future Enhancements
While the current platform is a powerful proof-of-concept, several key areas are identified for future development to evolve it into a fully-featured, production-ready service:

- **Transition to a Fully AWS-Native Streaming Architecture**  
  The current simulation of the Idealista data stream using Apache Kafka could be migrated to a fully managed, cloud-native solution using Amazon MSK (Managed Streaming for Kafka). This would enhance the reliability, scalability, and integration of the real-time data ingestion pipeline.

- **Improve NLP Sentiment Models and Expand Scope**  
  The current sentiment analysis model could be enhanced with more advanced NLP techniques to better understand nuance, sarcasm, and context in social media posts. Furthermore, the sentiment analysis could be expanded to include data from other platforms (e.g., Twitter, real estate forums) to provide a more comprehensive view of market sentiment.

- **Develop Advanced Analytical and Predictive Features**  
  The analytical capabilities of the platform could be extended by developing more complex machine learning models. This includes creating models to forecast rental yields, predict neighborhood growth trends, and even use reinforcement learning to suggest optimal investment strategies based on user-defined risk profiles.


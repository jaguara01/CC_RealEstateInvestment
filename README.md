# InmoDecision: A Data-Driven Advisor for Real Estate Investment with Social Sentiment Analysis

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

  ![Architecture](https://github.com/jaguara01/CC_RealEstateInvestment/blob/main/img/Dash.png)

- **Listings Explorer**  
  Map of Barcelona with property filters (price, rooms, bathrooms, etc.)  

- **Sentiment Analysis**  
  Analyzes **Bluesky posts** for market sentiment  

*(Image: Investment dashboard showing property features, financing, and ROI summary)*

---

### 2.2. The Live Pulse: Real-Time Alerting System
Specialized app for **instant property alerts**:

- **Real-time Ingestion**  
  Property listings streamed into **Kafka**  

- **Real-time Prediction**  
  Python consumer fetches model from S3, compares price vs. predicted value  

- **Slack Notifications**  
  Alerts when undervalued properties appear  

*(Image: Slack alerts showing undervalued property notifications)*

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
- **IaC:** AWS CloudFormation  

---

## 6. Development Methodology
Follows **Twelve-Factor App** methodology:  
- Code in version control  
- Dependencies explicitly declared  
- Config via environment  
- Logs as event streams  

---

## 7. Challenges and Solutions

- **Network Connectivity:** Fixed ECS private subnet issue via NAT Gateway  
- **IAM Permissions:** Debugged failures by properly scoping IAM roles/policies  
- **Frontend Deployment:** Migrated from EC2 → Elastic Beanstalk; scaled instances for performance  

---

## 8. Conclusions and Future Work

**Achievements:**  
- Built a scalable, cloud-native real estate intelligence platform  
- Integrated diverse data sources & machine learning  
- Delivered actionable insights via dashboards & alerts  

**Future Enhancements:**  
- Full AWS-native streaming with **MSK (Managed Kafka)**  
- Improved **NLP sentiment models** across more platforms  
- Advanced analytics: rental yields & neighborhood growth predictions  

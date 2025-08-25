# InmoDecision: A Data-Driven Advisor for Real Estate Investment with Social Sentiment Analysis

## 1. Project Overview and Goals

### The Challenge: Navigating the Modern Real Estate Market
In today's volatile real estate market, particularly in major European cities like **Barcelona**, making informed investment or purchasing decisions is more challenging than ever. Potential buyers and investors are faced with a deluge of fragmented data from various sources, making it difficult to gauge a property's true value, understand market trends, or anticipate future opportunities.  

The critical missing piece in the market is a unified platform that contextualizes **quantitative data** (like property prices and specifications) with **qualitative insights** (such as public sentiment and neighborhood buzz).

### The Solution: InmoDecision
InmoDecision was conceived to address this market gap. It is an **end-to-end Big Data application** that serves as a comprehensive decision-making platform for the Barcelona real estate market.  

The project moves beyond simple property listings by building and deploying a sophisticated, **cloud-native system** that ingests, processes, and analyzes large volumes of data from disparate sources in real-time.

The result is a powerful tool that empowers users with a multi-faceted view of any property, enabling them to:
- Identify undervalued assets  
- Analyze investment profitability  
- Understand the social sentiment surrounding different neighborhoods  

### Project Goals and Objectives
This project was designed to demonstrate a mastery of **modern data engineering, cloud computing, and data science principles**.  

Primary objectives:
1. **Design and Implement a Scalable Big Data Architecture**  
   - Robust, multi-layered data pipeline on AWS  
   - Landing Zone (raw data), Trusted Zone (clean data), Exploitation Zone (analytics-ready data)  

2. **Develop a Real-Time Data Streaming and Processing System**  
   - Ingest and analyze real-time data from **Bluesky APIs** and **Apache Kafka**  

3. **Build and Deploy a Predictive Machine Learning Model**  
   - Price prediction model integrated into the application  

4. **Create a User-Facing Data Application**  
   - Interactive **Streamlit dashboard** for exploration and investment analysis  

5. **Demonstrate Cloud-Native and DevOps Best Practices**  
   - Serverless-first (AWS Fargate, Redshift Serverless), Docker, CloudFormation, GitHub Actions  

---

## 2. Data Consumption and Applications

The project culminates in a suite of applications designed to provide predictive analytics, interactive exploration, and real-time alerts.

### 2.1. Streamlit Web Application - Investment Analyzer
An **interactive web application** built with Streamlit for user-friendly property analysis:

- **Price Predictor**  
  Machine learning model trained on historical listings to predict fair market value.  

- **Real Estate Investment Plan**  
  Profitability assessment with projected cash flows and return analysis.  

- **Property Listings Explorer**  
  Interactive **map of Barcelona** with filters (price, rooms, bathrooms, etc.).  

- **Sentiment Analysis**  
  Analysis of **Bluesky social media posts** to extract real-time market sentiment.  

*(Image: Investment dashboard showing property features, loan financing, and investment summary)*

### 2.2. The Live Pulse: Real-time Alerting System
A specialized real-time system for **instant property investment alerts**:

- **Real-time Data Ingestion**  
  New listings streamed into **Apache Kafka topics**  

- **Real-time Prediction & Alerting**  
  Python consumer retrieves model from S3, predicts price, compares with listing  

- **Instant Notification with Slack**  
  Alerts on undervalued properties sent by **Slack bot**  

*(Image: Slack alerts showing undervalued property notifications)*

---

## 3. Data Architecture: From Raw Data to Actionable Insights

*(Image: Project Data Flow Diagram from Landing Zone to Exploitation Zone)*

### 3.1. Landing Zone (Amazon S3)
- Raw data stored in **S3 buckets** (structured, semi-structured, unstructured).  

### 3.2. Trusted Zone (Amazon S3 + Apache Spark)
- Data cleaned and validated using **Apache Spark**.  
- Stored in **Parquet format** for optimized analytics.  

**Key transformations:**
- Structured: schema correction, normalization, integration  
- Semi-structured: flattening JSON, extracting key fields  
- Unstructured: anonymization, sanitization, metadata extraction  

### 3.3. Exploitation Zone (Amazon Redshift)
- Final structured layer optimized for **analytics and BI**.  
- **Amazon Redshift Serverless** handles scaling and queries.  
- ETL from Trusted Zone into subject-based schemas.  

---

## 4. Cloud Infrastructure and Deployment

- **Containerization:** Docker for consistency  
- **Compute:** AWS Fargate & ECS for serverless orchestration  
- **Frontend:** Streamlit app deployed on AWS Elastic Beanstalk  
- **CI/CD:** GitHub Actions + Amazon ECR  
- **Networking:** Amazon VPC with subnets and NAT Gateway  
- **Monitoring & Security:** CloudWatch logs, IAM policies  
- **Infrastructure as Code:** AWS CloudFormation  

---

## 5. Development Methodology
The project followed **Twelve-Factor App principles**:  
- Single codebase in version control  
- Explicit dependency management  
- Configuration in the environment  
- Logs as event streams  

---

## 6. Challenges and Solutions

- **Network Connectivity:** Fixed by provisioning NAT Gateway and updating route tables.  
- **IAM Permissions:** Resolved by scoping IAM roles and policies correctly.  
- **Frontend Deployment:** Shifted from EC2 to Elastic Beanstalk, improved performance with upgraded instance types.  

---

## 7. Conclusions and Future Work

The InmoDecision project demonstrates how to integrate diverse data sources and cloud technologies into a **scalable, secure, and valuable real estate investment tool**.

**Future enhancements:**
- Full cloud integration of **real-time Idealista listings** with AWS MSK  
- Improved **NLP sentiment analysis** across multiple platforms  
- Advanced analytics for **rental yields** and **neighborhood growth predictions**  


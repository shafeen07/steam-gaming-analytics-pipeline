# Steam Gaming Analytics Pipeline
## Real-time Data Engineering & Business Intelligence Portfolio Project

### Project Overview

This project demonstrates end-to-end data engineering capabilities through a production-ready real-time analytics pipeline for Steam gaming data. The system collects player counts and community reviews, processes them with local machine learning models, orchestrates workflows, and delivers business intelligence through interactive dashboards.

**Duration:** 7 days of data collection (September 18-25, 2025)  
**Data Volume:** 100,000+ records processed  
**Architecture:** Containerized microservices with real-time processing

---

### Technical Architecture

#### System Components

**Data Collection Layer:**
- Real-time Steam Web API integration
- Python-based data collection services
- Containerized with Docker for scalability

**Processing Layer:**
- PostgreSQL database with optimized time-series schemas
- Local LLM sentiment analysis using CardiffNLP RoBERTa model
- Automated data quality monitoring

**Orchestration Layer:**
- Apache Airflow for workflow management
- Automated monitoring and alerting
- Data pipeline health checks

**Visualization Layer:**
- Power BI with DirectQuery and Import modes
- Real-time operational dashboards
- Historical trend analysis

#### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Collection** | Python, Requests | Steam API integration |
| **Containerization** | Docker, Docker Compose | Service orchestration |
| **Database** | PostgreSQL 13 | Time-series data storage |
| **ML Processing** | Hugging Face Transformers | Sentiment analysis |
| **Workflow Orchestration** | Apache Airflow 2.7.1 | Pipeline management |
| **Business Intelligence** | Microsoft Power BI | Data visualization |
| **Infrastructure** | Local development environment | Cost-effective prototyping |

---

### Data Architecture

#### Database Schema

**Core Tables:**
- `steam_player_tracking` - Real-time player count data
- `steam_review_tracking` - Community reviews with sentiment analysis
- `game_tracking_config` - System configuration and monitoring
- `pipeline_monitoring_reports` - Operational health metrics

**Key Features:**
- Optimized indexes for time-series queries
- Automated data retention policies
- Foreign key relationships for referential integrity

#### Data Flow

```
Steam API → Python Collectors → PostgreSQL → Sentiment Analysis → Power BI Dashboards
     ↓              ↓               ↓              ↓              ↓
API Requests → Data Processing → Storage → ML Analysis → Business Intelligence
```

---

### Machine Learning Implementation

#### Sentiment Analysis Pipeline

**Model:** CardiffNLP Twitter-RoBERTa-base-sentiment-latest  
**Fallback:** DistilBERT base uncased (SST-2)  
**Processing:** Local inference with GPU acceleration when available

**Output Schema:**
- `sentiment_score`: -1.0 (negative) to +1.0 (positive)
- `sentiment_label`: categorical classification (positive/negative/neutral)
- `sentiment_confidence`: model confidence score
- `processing_time_ms`: performance metrics

**Performance Metrics:**
- Average processing time: 2.5 seconds per review
- Confidence threshold: 0.3 minimum for classification
- Success rate: 95%+ processing completion

---

### Business Intelligence Insights

#### Gaming Activity Patterns

**Peak Hours Analysis:**
- GTA V: 10-11 AM CST (global morning peak)
- Sea of Thieves: 2-3 PM CST (afternoon engagement)
- Team Fortress 2: 1-2 PM CST (lunch break gaming)

**Weekly Engagement Trends:**
- Weekend effect: 25% higher activity on Saturday/Sunday
- Thursday-Sunday buildup in player engagement
- Different games serve distinct global audiences

#### Community Sentiment Analysis

**Sentiment Distribution:**
- Team Fortress 2: 0.31 average sentiment (62% positive reviews)
- Sea of Thieves: 0.12 average sentiment (52% positive reviews)
- GTA V: 0.04 average sentiment (mixed community feedback)

**Business Value:**
- Early identification of community health issues
- Content strategy optimization insights
- Player satisfaction monitoring

---

### Operational Monitoring

#### Airflow Workflow Management

**Data Quality Monitoring DAG:**
- Hourly execution schedule
- Multi-table health checks
- Automated anomaly detection
- Performance trend analysis

**Key Monitoring Metrics:**
- Data freshness (< 15 minutes for player counts)
- API response times (< 3 seconds average)
- Processing backlogs and error rates
- Database performance indicators

#### Production Readiness Features

**Reliability:**
- Automated retry mechanisms
- Error handling and logging
- Health check endpoints
- Graceful degradation

**Scalability:**
- Containerized microservices
- Database connection pooling
- Horizontal scaling capability
- Resource usage monitoring

---

### Portfolio Demonstrations

#### Dashboard Categories

**1. Global Gaming Activity Analysis**
- Real-time player count heatmaps
- Day-of-week engagement patterns
- Peak hour identification across games
- Global timezone distribution insights

**2. Community Sentiment Analysis**
- ML-powered sentiment scoring
- Game-by-game community health metrics
- Review volume and quality analysis
- Executive summary KPI cards

**3. Operational Monitoring**
- Pipeline health indicators
- Data quality metrics
- Processing performance dashboards
- System reliability reporting

#### Technical Capabilities Demonstrated

**Data Engineering Core Skills:**
- API integration and real-time data collection
- Database design and optimization
- ETL pipeline development
- Data quality assurance

**Modern Data Stack:**
- Containerized microservices architecture
- Machine learning operations (MLOps)
- Workflow orchestration
- Cloud-native development practices

**Business Intelligence:**
- Executive dashboard development
- Data storytelling and visualization
- Performance metrics and KPIs
- Actionable insights generation

---

### Project Outcomes

#### Technical Achievements

**Infrastructure:**
- Built production-ready data pipeline processing 100K+ records
- Implemented local LLM sentiment analysis with 95%+ reliability
- Created automated monitoring and alerting systems
- Designed scalable containerized architecture

**Data Processing:**
- Collected comprehensive gaming activity dataset
- Processed community reviews with ML sentiment analysis
- Implemented time-series optimization for performance
- Established data quality and governance standards

#### Business Impact

**Market Insights:**
- Identified distinct gaming audience segments
- Revealed global engagement patterns
- Quantified community sentiment trends
- Enabled data-driven decision making

**Operational Excellence:**
- Automated data quality monitoring
- Reduced manual oversight requirements
- Implemented proactive alerting systems
- Established production monitoring standards

---

### Future Enhancements

#### Technical Improvements
- Cloud deployment (AWS/Azure/GCP)
- Real-time streaming with Apache Kafka
- Advanced ML models for player behavior prediction
- API rate limiting and cost optimization

#### Business Intelligence Extensions
- Competitive analysis dashboards
- Player lifetime value modeling
- Market trend forecasting
- Regional engagement analysis

---

### Project Repository Structure

```
steam-analytics-pipeline/
├── docker-compose.yaml           # Service orchestration
├── airflow/
│   └── dags/                     # Workflow definitions
├── kafka/                        # Data processing services
│   ├── dynamic_game_tracker.py   # Data collection
│   └── database_sentiment_processor.py  # ML processing
├── data/                         # Generated datasets
├── dashboards/                   # Power BI files
└── documentation/                # Project documentation
```

### Contact Information

This portfolio project demonstrates comprehensive data engineering capabilities suitable for modern data-driven organizations. The combination of real-time processing, machine learning integration, and business intelligence delivery showcases end-to-end technical and analytical skills.

**Key Differentiators:**
- Production-ready architecture and monitoring
- Local ML implementation reducing operational costs
- Executive-ready business intelligence delivery
- Comprehensive documentation and testing

---

*This project serves as a practical demonstration of data engineering expertise, combining technical depth with business value delivery in a realistic industry application.*
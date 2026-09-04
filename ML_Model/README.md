# Customer Churn Prediction Model

## Overview

This folder contains the Machine Learning model developed to predict customer churn as part of the **Customer Churn ETL Pipeline**.

The model uses customer value, support, product, activity, and usage features to estimate whether a customer is likely to churn.

The model is the **consumer layer** of the data pipeline and uses prepared customer features produced by the upstream ETL process.

## Objective

The main objective is to build a **binary classification model** that predicts customer churn.

| Prediction | Meaning |
|---|---|
| `0` | Customer is not expected to churn |
| `1` | Customer is expected to churn |

The model also provides a **churn probability** for each customer.

## Position in the Data Pipeline

```text
Source Data
    ↓
NiFi / Sqoop
    ↓
Bronze Layer
    ↓
bronze_to_silver.py
    ↓
Silver Layer
    ↓
silver_to_gold.py
    ↓
Gold Layer
    ↓
Customer Features
    ↓
ML Churn Prediction Model
    ↓
Churn Prediction + Probability
```

## Model Input Features

The final model uses **5 selected features**:

| Feature | Description |
|---|---|
| `CLV_LTV` | Customer lifetime value |
| `avg_ticket_res_time_hrs` | Average support ticket resolution time |
| `num_products` | Number of products associated with the customer |
| `months_active` | Number of months the customer has been active |
| `total_usage` | Total customer usage |

These features represent:

- **Customer Value** → `CLV_LTV`
- **Support Experience** → `avg_ticket_res_time_hrs`
- **Product Adoption** → `num_products`
- **Customer Tenure** → `months_active`
- **Customer Engagement** → `total_usage`

## Feature Selection

Two feature sets were compared:

- **Baseline:** 16 features
- **Final:** 5 selected features

The final feature set was selected because it achieved a better ROC-AUC score while using fewer features.

## Model Evaluation

| Metric | Baseline (16 Features) | Final (5 Features) | Difference |
|---|---:|---:|---:|
| Accuracy | 0.7800 | 0.7500 | -0.0300 |
| ROC-AUC | 0.6608 | 0.6964 | +0.0356 |

The final model has slightly lower accuracy, but its ROC-AUC improved from **0.6608 to 0.6964**.

This indicates better overall discrimination between churned and non-churned customers.

## Prediction Output

The model generates customer-level predictions containing:

```text
customer_id
actual_churn
predicted_churn
churn_probability
```

Example:

```text
customer_id | actual_churn | predicted_churn | churn_probability
------------|--------------|-----------------|------------------
10025       | 1            | 1               | 0.84
10026       | 0            | 0               | 0.18
10027       | 1            | 0               | 0.43
```

The churn probability can be used to rank customers according to their estimated churn risk.

## Business Value

The model can help the business:

- Identify customers at high risk of churn.
- Prioritize retention campaigns.
- Focus resources on potentially valuable at-risk customers.
- Understand customer behavior related to churn.
- Support data-driven retention decisions.
- Combine ML predictions with downstream analytics.

## Model Development Workflow

```text
1. Load Data
      ↓
2. Data Preparation
      ↓
3. Feature Selection
      ↓
4. Model Training
      ↓
5. Model Evaluation
      ↓
6. Compare Feature Sets
      ↓
7. Select Final Features
      ↓
8. Generate Predictions
```

The complete workflow is available in the accompanying Jupyter Notebook.

## Notebook

Place the model development notebook in this folder and link it here:

```md
[Open the ML Model Notebook](./your_notebook_name.ipynb)
```

Replace `your_notebook_name.ipynb` with the actual notebook filename.

## Suggested Folder Structure

```text
ML_Model/
├── README.md
├── churn_model.ipynb
├── data/
├── models/
└── predictions/
```

## Integration with the ETL Pipeline

The ML model consumes features produced by the Gold layer:

```text
                 GOLD LAYER
                     │
                     ▼
             Customer Features
                     │
          ┌──────────┼──────────┐
          │          │          │
       Value      Activity    Usage
          │          │          │
          └──────────┼──────────┘
                     ▼
          Customer Churn Model
                     │
             ┌───────┴───────┐
             ▼               ▼
      Predicted Churn   Churn Probability
             │               │
             └───────┬───────┘
                     ▼
              Retention Actions
```

## Technologies

- Python
- Jupyter Notebook
- Pandas
- NumPy
- Scikit-learn
- Matplotlib

## Key Metrics

```text
Final Features: 5
Baseline Features: 16

Final Accuracy: 75.00%
Final ROC-AUC: 0.6964

Baseline Accuracy: 78.00%
Baseline ROC-AUC: 0.6608
```

## Model Consumer

The ML model acts as a consumer of the data engineering pipeline.

It receives trusted, transformed customer features from the Gold layer and converts them into churn predictions that can support business retention decisions.

## Important Note

The model is intended to support customer retention analysis and decision-making. Predictions should be considered together with business context and other customer analytics.

## Summary

The Customer Churn Prediction Model is the ML consumer of the ETL pipeline.

It uses five selected customer features to predict churn and generate a probability score.

**Final Accuracy:** 75.00%  
**Final ROC-AUC:** 0.6964

The model helps transform the output of the data engineering pipeline into actionable customer churn insights.

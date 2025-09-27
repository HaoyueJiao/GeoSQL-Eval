## **GeoSQL-Eval Framework  Introduction**

**GeoSQL-Eval** is an end-to-end automated evaluation framework designed to systematically assess the performance of large language models (LLMs) in generating PostGIS queries. The core modules of the framework include **GeoSQL-Bench**, **GeoSQL-Generate**, **GeoSQL-Eval-Syntax-Level**, **GeoSQL-Eval-Knowledge-Level**, and **GeoSQL-Eval-Table-Schema-Level**. The first module is for model-generated GeoSQL statements, the second is the benchmark dataset, and the latter three are the primary evaluation modules, each covering different evaluation dimensions, from syntax generation to query execution, comprehensively covering all aspects of GeoSQL generation.

### 1. **GeoSQL-Bench**

- **function_signatures.json**: Contains all PostGIS function signatures, used for function recognition and parameter matching.
- **Multiple_Choice.json**: Multiple choice tasks to evaluate the model's understanding of basic spatial queries.
- **TF_Question.json**: True/False questions to assess the model's understanding of basic query rules.
- **Syntax-level_SQL_Generation_Question_Explicit.json**: Explicit syntax-level SQL generation tasks.
- **Syntax-level_SQL_Generation_Question_Underspecified.json**: Ambiguous syntax-level SQL generation tasks to evaluate model reasoning under incomplete descriptions.
- **Table_Schema_Retrieval_Question_Explicit.json**: Explicit table schema retrieval questions to evaluate the model’s understanding of database schema.
- **Table_Schema_Retrieval_Question_Underspecified.json**: Ambiguous table schema retrieval questions to test the model’s reasoning ability.
- **Table_Schema_Retrieval_Question_table&column_picked.json**: Questions related to the involved tables and columns for each task.

### 2. **GeoSQL-Generate**

**GeoSQL-Generate** is used to generate PostGIS GeoSQL queries. By running `*Generate.py`, you can select different models to generate GeoSQL queries for corresponding tasks. Main code files include:

- **Select_Knowledge_Generate.py**: Generates answers for Knowledge-level multiple-choice questions.
- **Judgment_Knowledge_Generate.py**: Generates answers for Knowledge-level true/false questions.
- **GeoSQL_Syntax_Generate.py**: Generates SQL queries based on syntax-level questions.
- **GeoSQL_Table_Schema_Generate.py**: Generates GeoSQL queries based on table schema-level questions.

When running `Generate.py`, configure the model selection and ensure the paths point to the correct test datasets.

- **call_language_model.py**: Core function for interacting with the language model to generate GeoSQL queries.
- **llm_config.yaml**: Configuration file storing model selections and keys.

### 3. **GeoSQL-Eval-Knowledge-Level**

**GeoSQL-Eval-Knowledge-Level** focuses on evaluating the model’s ability to understand PostGIS functions and basic spatial queries.

Main files include:

- **clean.py**: Data cleaning script.
- **eval_judgment.py**: Evaluates the model's understanding of PostGIS functions and parameters.
- **summary_judgment.py**: Summarizes knowledge-level evaluation results.
- **eval_select.py**: Executes PostGIS query selection operations and generates evaluation results.
- **summary_select.py**: Summarizes query selection results.

### 4. **GeoSQL-Eval-Syntax-Level**

**GeoSQL-Eval-Syntax-Level** evaluates the syntax correctness and execution outcomes of the generated GeoSQL queries. Key files include:

- **eval.py**: Core evaluation script, running evaluation tasks.
- **reorder_data.py**: Reorders evaluation data.
- **clean.py**, **deduplicate.py**: Data cleaning and deduplication scripts.
- **main_eval_\*_eval.py**: Evaluation execution files for different layers.
- **evaluate_\*.py**: Evaluation tool functions.
- **eval_summary_\*.py**: Generates evaluation reports, providing detailed results.

### 5. **GeoSQL-Eval-Table-Schema-Level**

**GeoSQL-Eval-Table-Schema-Level** evaluates the model's ability to generate queries related to database schema. Key files include:

- **eval.py**: Core evaluation script, running evaluation tasks.
- **reorder_data.py**: Reorders evaluation data.
- **clean.py**, **deduplicate.py**: Data cleaning and deduplication scripts.
- **DB_ID.py**: Adds the database name.
- **main_eval_\*_eval.py**: Evaluation execution files for different layers.
- **evaluate_\*.py**, **pick_by_tableschema.py**: Evaluation tool functions.
- **eval_summary_\*.py**: Generates evaluation reports.

### 6. **Error Type Analysis**

The error type analysis module is located in the `GeoSQL-Eval-Syntax-Level/Error_Type_Eval` and `GeoSQL-Eval-Table-Schema-Level/Error_Type_Eval` folders and is divided into two main steps:

- **Step 1: Error Type Judgment**: Run `error_judgment_LLM_all.py` to classify and identify error types in the generated SQL.
- **Step 2: Error Summary**: Run `error_type_summary.py` to generate a summary table of error types.

### 7. **Configure Database Connection**

For SQL execution and evaluation, you need to configure the database connection in the relevant evaluation files.

### 8. **Configure Model Address and Key**

The model selection and key configuration are stored in the `llm_config.yaml` file, ensuring proper access to the selected model.

### 9. **File Directory Overview**

```
GeoSQL-Eval/
├── GeoSQL-Bench/
│   ├── function_signatures.json  # Contains PostGIS function signatures for function recognition and parameter matching
│   ├── Multiple_Choice.jsonl    # Multiple choice tasks for evaluating spatial query knowledge
│   ├── Syntax-level_SQL_Generation_Question_Explicit.jsonl  # Explicit syntax-level SQL generation tasks
│   ├── Syntax-level_SQL_Generation_Question_Underspecified.jsonl  # Ambiguous SQL generation tasks
│   ├── Table_Schema_Retrieval_Question_Explicit.jsonl  # Explicit table schema retrieval questions
│   ├── Table_Schema_Retrieval_Question_table&column_picked.jsonl  # Questions related to tables and columns
│   ├── Table_Schema_Retrieval_Question_Underspecified.jsonl  # Ambiguous table schema retrieval questions
│   └── TF_Question.jsonl        # True/False questions
│
├── GeoSQL-Eval-Knowledge-Level/
│   ├── clean.py                 # Data cleaning script
│   ├── eval_judgment.py         # Judgment task evaluation
│   ├── eval_select.py           # Query selection task evaluation
│   ├── summary_judgment.py      # Summarizes judgment task evaluation results
│   └── summary_select.py        # Summarizes query selection task results
│
├── GeoSQL-Eval-Syntax-Level/
│   ├── Error_Type_Eval/         # Error type evaluation module
│   │   ├── call_language_model.py  # Calls language model to generate SQL queries
│   │   ├── error_judgment_LLM_all.py  # Identifies and classifies errors in SQL
│   │   ├── error_type_summary.py    # Summarizes error types and generates reports
│   │   └── llm_config.yaml         # Configuration file for language model parameters and keys
│   ├── clean.py                 # Data cleaning script
│   ├── deduplicate.py           # Data deduplication script
│   ├── eval.py                  # Core evaluation script
│   ├── eval_summary_execution.py  # Evaluates SQL query execution results
│   ├── eval_summary_resource_usage.py  # Resource usage evaluation report
│   ├── eval_summary_semantic_pgtype.py  # Semantic consistency evaluation report
│   ├── eval_summary_with_passn.py  # Evaluation report with pass rate
│   ├── evaluate_execution.py     # Evaluates SQL query execution
│   ├── evaluate_semantic_pgtype.py  # Evaluates semantic consistency
│   ├── main_eval_execution_eval.py  # Executes SQL evaluation
│   ├── main_eval_semantic_pgtype_eval.py  # Evaluates semantic alignment and parameter matching
│   ├── reorder_data.py           # Reorders evaluation data
│   └── summary.py                # Generates evaluation summary report
│
├── GeoSQL-Eval-Table-Schema-Level/
│   ├── Error_Type_Eval/         # Error type evaluation module
│   │   ├── call_language_model.py  # Calls language model to generate queries
│   │   ├── error_judgment_LLM_all.py  # Classifies errors in generated SQL
│   │   ├── error_type_summary.py    # Summarizes error types and generates reports
│   │   └── llm_config.yaml         # Configuration file for language model parameters and keys
│   ├── clean.py                 # Data cleaning script
│   ├── DB_ID.py                 # Configures database name
│   ├── deduplicate.py           # Data deduplication script
│   ├── eval.py                  # Core evaluation script
│   ├── eval_summary_execution.py  # Evaluates SQL query execution
│   ├── eval_summary_resource_usage.py  # Resource usage evaluation
│   ├── eval_summary_semantic_pgtype.py  # Semantic evaluation report
│   ├── eval_summary_with_passn.py  # Evaluation with pass rate
│   ├── evaluate_execution.py     # Evaluates execution of SQL queries
│   ├── evaluate_semantic_pgtype.py  # Evaluates semantic consistency of queries
│   ├── main_eval_execution_eval.py  # Executes SQL evaluation
│   ├── main_eval_semantic_pgtype_eval.py  # Evaluates semantic alignment
│   ├── main_eval_table_column_hits_eval.py  # Evaluates column hits accuracy
│   ├── pick_by_tableschema.py    # Extracts queries by table schema
│   ├── reorder_data.py           # Reorders data
│   └── summary.py                # Generates evaluation summary
│
└── GeoSQL-Generate/
	├── call_language_model.py     # Calls language model to generate GeoSQL queries
	├── GeoSQL_Syntax_Generate.py  # Syntax-based GeoSQL query generation
	├── GeoSQL_Table_Schema_Generate.py  # Table schema-based GeoSQL query generation
	├── Judgment_Knowledge_Generate.py  # Generates judgment task answers
	├── llm_config.yaml            # Configuration file for language model parameters and keys
	└── Select_Knowledge_Generate.py  # Generates selection task answers

```


WITH

companies AS (
  SELECT * FROM gi.general.dim_companies
  WHERE data_source = 'UAU-SERVMAIS'
),

installment_types AS(
  SELECT * FROM gi.general.dim_installment_types
  WHERE data_source = 'UAU-SERVMAIS'
),

sales AS (
  SELECT * FROM gi.sales.dim_sales_contracts 
  WHERE data_source = 'UAU-SERVMAIS'
    AND snapshot_date = current_date()-1
),

income AS (
  SELECT * FROM gi.financial.rpt_income
  WHERE data_source = 'UAU-SERVMAIS'
    AND snapshot_date = current_date()-1
)

SELECT
  companies.company_id,
  companies.company_description,
  installment_types.installment_type_id,
  installment_types.installment_type_description,
  sales.enterprise_id,
  sales.sk_sale,
  sales.sale_id,
  income.installment_id,
  income.due_date,
  income.received_installment_date,
  income.principal_amount,
  income.interest_amount,
  income.correction_amount,
  income.total_amount,
  income.anticipated_installment_amount,
  income.data_source,
  income.snapshot_date,
  income.account_type_description
FROM income
INNER JOIN companies
  ON companies.sk_company = income.sk_company
INNER JOIN installment_types
  ON installment_types.sk_installment_type = income.sk_installment_type
INNER JOIN sales
  ON income.sk_sale = sales.sk_sale
WHERE 1=1
  AND companies.company_id = 331
  -- inserir código UAU acima
  AND received_installment_date IS NOT null
  and account_type_description = 'CONTA CORRENTE'
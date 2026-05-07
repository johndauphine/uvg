-- ============================================================
-- crm_mssql.sql — 3NF CRM source schema, native SQL Server idioms
--
-- Engine-specific features exercised (for SMT migration testing):
--   * IDENTITY(1,1) on INT and BIGINT
--   * DATETIME2 (high-precision wall clock)
--   * DATETIMEOFFSET (timezone-aware)
--   * UNIQUEIDENTIFIER with DEFAULT NEWID()
--   * NVARCHAR(MAX) / VARCHAR(MAX)
--   * BIT for booleans
--   * AS (expression) PERSISTED computed columns
--   * GETUTCDATE() / SYSDATETIMEOFFSET() function defaults
--   * Composite PK and composite FK to composite PK
--   * Self-referencing FKs (departments, employees, product_categories)
--   * CASCADE / SET NULL / NO ACTION mix
--   * CHECK constraints (IN, BETWEEN, UPPER, multi-column)
--   * Large text in audit log
-- ============================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE Companies (
    id            INT IDENTITY(1,1)        NOT NULL PRIMARY KEY,
    code          VARCHAR(20)              NOT NULL UNIQUE,
    name          NVARCHAR(200)            NOT NULL,
    industry      VARCHAR(50)              NULL,
    founded_date  DATE                     NULL,
    is_active     BIT                      NOT NULL CONSTRAINT DF_Companies_active  DEFAULT 1,
    created_at    DATETIME2                NOT NULL CONSTRAINT DF_Companies_created DEFAULT GETUTCDATE(),
    updated_at    DATETIME2                NOT NULL CONSTRAINT DF_Companies_updated DEFAULT GETUTCDATE(),
    CONSTRAINT CK_Companies_code_upper CHECK (code = UPPER(code))
);

CREATE TABLE Departments (
    id            INT IDENTITY(1,1)        NOT NULL PRIMARY KEY,
    company_id    INT                      NOT NULL,
    parent_id     INT                      NULL,
    name          NVARCHAR(150)            NOT NULL,
    code          VARCHAR(20)              NOT NULL,
    cost_center   VARCHAR(20)              NULL,
    is_active     BIT                      NOT NULL CONSTRAINT DF_Dept_active  DEFAULT 1,
    created_at    DATETIME2                NOT NULL CONSTRAINT DF_Dept_created DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Dept_Company  FOREIGN KEY (company_id) REFERENCES Companies(id),
    CONSTRAINT FK_Dept_Parent   FOREIGN KEY (parent_id)  REFERENCES Departments(id),
    CONSTRAINT UQ_Dept_Code     UNIQUE (company_id, code),
    CONSTRAINT CK_Dept_code_up  CHECK (code = UPPER(code))
);

CREATE TABLE Employees (
    id                INT IDENTITY(1,1)    NOT NULL PRIMARY KEY,
    company_id        INT                  NOT NULL,
    department_id     INT                  NULL,
    manager_id        INT                  NULL,
    employee_uuid     UNIQUEIDENTIFIER     NOT NULL CONSTRAINT DF_Emp_uuid DEFAULT NEWID(),
    employee_no       VARCHAR(20)          NOT NULL,
    first_name        NVARCHAR(100)        NOT NULL,
    last_name         NVARCHAR(100)        NOT NULL,
    email             VARCHAR(255)         NOT NULL,
    phone             VARCHAR(50)          NULL,
    job_title         NVARCHAR(150)        NULL,
    salary            DECIMAL(12,2)        NULL,
    hire_date         DATETIMEOFFSET       NOT NULL CONSTRAINT DF_Emp_hire DEFAULT SYSDATETIMEOFFSET(),
    termination_date  DATETIMEOFFSET       NULL,
    is_active         BIT                  NOT NULL CONSTRAINT DF_Emp_active  DEFAULT 1,
    created_at        DATETIME2            NOT NULL CONSTRAINT DF_Emp_created DEFAULT GETUTCDATE(),
    updated_at        DATETIME2            NOT NULL CONSTRAINT DF_Emp_updated DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Emp_Company FOREIGN KEY (company_id)    REFERENCES Companies(id),
    CONSTRAINT FK_Emp_Dept    FOREIGN KEY (department_id) REFERENCES Departments(id),
    CONSTRAINT FK_Emp_Manager FOREIGN KEY (manager_id)    REFERENCES Employees(id),
    CONSTRAINT UQ_Emp_No      UNIQUE (company_id, employee_no),
    CONSTRAINT UQ_Emp_Email   UNIQUE (email),
    CONSTRAINT UQ_Emp_Uuid    UNIQUE (employee_uuid),
    CONSTRAINT CK_Emp_salary  CHECK (salary IS NULL OR salary >= 0),
    CONSTRAINT CK_Emp_term    CHECK (termination_date IS NULL OR termination_date >= hire_date)
);

CREATE TABLE Customers (
    id                    INT IDENTITY(1,1)    NOT NULL PRIMARY KEY,
    company_id            INT                  NOT NULL,
    external_id           UNIQUEIDENTIFIER     NOT NULL CONSTRAINT DF_Cust_ext     DEFAULT NEWID(),
    customer_code         VARCHAR(20)          NOT NULL,
    customer_type         VARCHAR(20)          NOT NULL CONSTRAINT DF_Cust_type    DEFAULT 'individual',
    company_name          NVARCHAR(200)        NULL,
    first_name            NVARCHAR(100)        NULL,
    last_name             NVARCHAR(100)        NULL,
    email                 VARCHAR(255)         NOT NULL,
    phone                 VARCHAR(50)          NULL,
    fax                   VARCHAR(50)          NULL,
    website               VARCHAR(500)         NULL,
    tax_id                VARCHAR(50)          NULL,
    credit_limit          DECIMAL(15,2)        NOT NULL CONSTRAINT DF_Cust_credit  DEFAULT 0,
    notes                 NVARCHAR(MAX)        NULL,
    assigned_employee_id  INT                  NULL,
    is_active             BIT                  NOT NULL CONSTRAINT DF_Cust_active  DEFAULT 1,
    is_deleted            BIT                  NOT NULL CONSTRAINT DF_Cust_deleted DEFAULT 0,
    deleted_at            DATETIME2            NULL,
    created_at            DATETIME2            NOT NULL CONSTRAINT DF_Cust_created DEFAULT GETUTCDATE(),
    updated_at            DATETIME2            NOT NULL CONSTRAINT DF_Cust_updated DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Cust_Company   FOREIGN KEY (company_id)           REFERENCES Companies(id),
    CONSTRAINT FK_Cust_Employee  FOREIGN KEY (assigned_employee_id) REFERENCES Employees(id) ON DELETE SET NULL,
    CONSTRAINT UQ_Cust_Code      UNIQUE (company_id, customer_code),
    CONSTRAINT UQ_Cust_External  UNIQUE (external_id),
    CONSTRAINT UQ_Cust_Email     UNIQUE (email),
    CONSTRAINT CK_Cust_type      CHECK (customer_type IN ('individual','company','government')),
    CONSTRAINT CK_Cust_credit    CHECK (credit_limit >= 0),
    CONSTRAINT CK_Cust_identity  CHECK (
        (customer_type = 'individual' AND first_name IS NOT NULL AND last_name IS NOT NULL)
     OR (customer_type IN ('company','government') AND company_name IS NOT NULL)
    )
);

CREATE TABLE CustomerAddresses (
    id              INT IDENTITY(1,1)    NOT NULL PRIMARY KEY,
    customer_id     INT                  NOT NULL,
    address_type    VARCHAR(20)          NOT NULL,
    line1           NVARCHAR(255)        NOT NULL,
    line2           NVARCHAR(255)        NULL,
    city            NVARCHAR(100)        NOT NULL,
    state_province  NVARCHAR(100)        NULL,
    postal_code     VARCHAR(20)          NULL,
    country_code    CHAR(2)              NOT NULL,
    is_primary      BIT                  NOT NULL CONSTRAINT DF_Addr_primary DEFAULT 0,
    CONSTRAINT FK_Addr_Customer FOREIGN KEY (customer_id) REFERENCES Customers(id) ON DELETE CASCADE,
    CONSTRAINT CK_Addr_type     CHECK (address_type IN ('billing','shipping','physical','mailing')),
    CONSTRAINT CK_Addr_country  CHECK (country_code = UPPER(country_code))
);

CREATE TABLE Contacts (
    id           INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    customer_id  INT               NOT NULL,
    first_name   NVARCHAR(100)     NOT NULL,
    last_name    NVARCHAR(100)     NOT NULL,
    title        NVARCHAR(100)     NULL,
    email        VARCHAR(255)      NULL,
    phone        VARCHAR(50)       NULL,
    is_primary   BIT               NOT NULL CONSTRAINT DF_Contact_primary DEFAULT 0,
    is_active    BIT               NOT NULL CONSTRAINT DF_Contact_active  DEFAULT 1,
    created_at   DATETIME2         NOT NULL CONSTRAINT DF_Contact_created DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Contact_Customer FOREIGN KEY (customer_id) REFERENCES Customers(id) ON DELETE CASCADE
);

CREATE TABLE ProductCategories (
    id          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    parent_id   INT               NULL,
    name        NVARCHAR(150)     NOT NULL,
    slug        VARCHAR(100)      NOT NULL UNIQUE,
    sort_order  INT               NOT NULL CONSTRAINT DF_PCat_sort DEFAULT 0,
    is_active   BIT               NOT NULL CONSTRAINT DF_PCat_active DEFAULT 1,
    CONSTRAINT FK_PCat_Parent  FOREIGN KEY (parent_id) REFERENCES ProductCategories(id),
    CONSTRAINT CK_PCat_slug_lo CHECK (slug = LOWER(slug))
);

CREATE TABLE Products (
    id            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    category_id   INT               NULL,
    sku           VARCHAR(50)       NOT NULL UNIQUE,
    name          NVARCHAR(255)     NOT NULL,
    description   NVARCHAR(MAX)     NULL,
    unit_price    DECIMAL(12,2)     NOT NULL,
    cost_price    DECIMAL(12,2)     NULL,
    currency      CHAR(3)           NOT NULL CONSTRAINT DF_Prod_curr    DEFAULT 'USD',
    weight_grams  INT               NULL,
    margin        AS (CASE WHEN cost_price IS NULL OR cost_price = 0 THEN NULL ELSE (unit_price - cost_price) / cost_price END) PERSISTED,
    is_taxable    BIT               NOT NULL CONSTRAINT DF_Prod_taxable DEFAULT 1,
    is_active     BIT               NOT NULL CONSTRAINT DF_Prod_active  DEFAULT 1,
    created_at    DATETIME2         NOT NULL CONSTRAINT DF_Prod_created DEFAULT GETUTCDATE(),
    updated_at    DATETIME2         NOT NULL CONSTRAINT DF_Prod_updated DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Prod_Category FOREIGN KEY (category_id) REFERENCES ProductCategories(id),
    CONSTRAINT CK_Prod_price    CHECK (unit_price >= 0),
    CONSTRAINT CK_Prod_cost     CHECK (cost_price IS NULL OR cost_price >= 0),
    CONSTRAINT CK_Prod_currency CHECK (currency = UPPER(currency)),
    CONSTRAINT CK_Prod_weight   CHECK (weight_grams IS NULL OR weight_grams > 0)
);

CREATE TABLE Orders (
    id             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    company_id     INT               NOT NULL,
    customer_id    INT               NOT NULL,
    order_no       VARCHAR(30)       NOT NULL,
    order_date     DATETIME2         NOT NULL CONSTRAINT DF_Order_date  DEFAULT GETUTCDATE(),
    required_date  DATETIME2         NULL,
    shipped_at     DATETIMEOFFSET    NULL,
    subtotal       DECIMAL(15,2)     NOT NULL CONSTRAINT DF_Order_sub   DEFAULT 0,
    tax_amount     DECIMAL(15,2)     NOT NULL CONSTRAINT DF_Order_tax   DEFAULT 0,
    total_amount   DECIMAL(15,2)     NOT NULL CONSTRAINT DF_Order_total DEFAULT 0,
    currency       CHAR(3)           NOT NULL CONSTRAINT DF_Order_curr  DEFAULT 'USD',
    status         VARCHAR(20)       NOT NULL CONSTRAINT DF_Order_st    DEFAULT 'pending',
    sales_rep_id   INT               NULL,
    notes          NVARCHAR(MAX)     NULL,
    created_at     DATETIME2         NOT NULL CONSTRAINT DF_Order_created DEFAULT GETUTCDATE(),
    updated_at     DATETIME2         NOT NULL CONSTRAINT DF_Order_updated DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Order_Company  FOREIGN KEY (company_id)   REFERENCES Companies(id),
    CONSTRAINT FK_Order_Customer FOREIGN KEY (customer_id)  REFERENCES Customers(id),
    CONSTRAINT FK_Order_Rep      FOREIGN KEY (sales_rep_id) REFERENCES Employees(id) ON DELETE SET NULL,
    CONSTRAINT UQ_Order_No       UNIQUE (company_id, order_no),
    CONSTRAINT CK_Order_required CHECK (required_date IS NULL OR required_date >= order_date),
    CONSTRAINT CK_Order_amounts  CHECK (subtotal >= 0 AND tax_amount >= 0 AND total_amount >= 0),
    CONSTRAINT CK_Order_total    CHECK (total_amount = subtotal + tax_amount),
    CONSTRAINT CK_Order_status   CHECK (status IN ('pending','paid','shipped','delivered','cancelled')),
    CONSTRAINT CK_Order_currency CHECK (currency = UPPER(currency))
);

CREATE TABLE OrderItems (
    order_id      INT             NOT NULL,
    line_no       INT             NOT NULL,
    product_id    INT             NOT NULL,
    quantity      DECIMAL(10,3)   NOT NULL,
    unit_price    DECIMAL(12,2)   NOT NULL,
    discount_pct  DECIMAL(5,2)    NOT NULL CONSTRAINT DF_OI_discount DEFAULT 0,
    line_total    AS (quantity * unit_price * (1 - discount_pct / 100)) PERSISTED,
    CONSTRAINT PK_OrderItems PRIMARY KEY (order_id, line_no),
    CONSTRAINT FK_OI_Order   FOREIGN KEY (order_id)   REFERENCES Orders(id) ON DELETE CASCADE,
    CONSTRAINT FK_OI_Product FOREIGN KEY (product_id) REFERENCES Products(id),
    CONSTRAINT CK_OI_qty     CHECK (quantity > 0),
    CONSTRAINT CK_OI_price   CHECK (unit_price >= 0),
    CONSTRAINT CK_OI_disc    CHECK (discount_pct BETWEEN 0 AND 100)
);

CREATE TABLE Invoices (
    id            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id      INT               NOT NULL,
    invoice_no    VARCHAR(30)       NOT NULL UNIQUE,
    issue_date    DATE              NOT NULL,
    due_date      DATE              NOT NULL,
    subtotal      DECIMAL(15,2)     NOT NULL,
    tax_amount    DECIMAL(15,2)     NOT NULL,
    total_amount  DECIMAL(15,2)     NOT NULL,
    currency      CHAR(3)           NOT NULL CONSTRAINT DF_Inv_curr DEFAULT 'USD',
    status        VARCHAR(20)       NOT NULL CONSTRAINT DF_Inv_st   DEFAULT 'draft',
    sent_at       DATETIMEOFFSET    NULL,
    paid_at       DATETIMEOFFSET    NULL,
    notes         NVARCHAR(MAX)     NULL,
    created_at    DATETIME2         NOT NULL CONSTRAINT DF_Inv_created DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Inv_Order  FOREIGN KEY (order_id) REFERENCES Orders(id),
    CONSTRAINT CK_Inv_due    CHECK (due_date >= issue_date),
    CONSTRAINT CK_Inv_amts   CHECK (subtotal >= 0 AND tax_amount >= 0 AND total_amount >= 0),
    CONSTRAINT CK_Inv_total  CHECK (total_amount = subtotal + tax_amount),
    CONSTRAINT CK_Inv_status CHECK (status IN ('draft','sent','paid','overdue','void')),
    CONSTRAINT CK_Inv_curr   CHECK (currency = UPPER(currency))
);

CREATE TABLE Payments (
    id            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    invoice_id    INT               NOT NULL,
    amount        DECIMAL(15,2)     NOT NULL,
    currency      CHAR(3)           NOT NULL CONSTRAINT DF_Pay_curr DEFAULT 'USD',
    method        VARCHAR(20)       NOT NULL,
    reference_no  VARCHAR(100)      NULL,
    paid_at       DATETIMEOFFSET    NOT NULL CONSTRAINT DF_Pay_at DEFAULT SYSDATETIMEOFFSET(),
    notes         NVARCHAR(MAX)     NULL,
    CONSTRAINT FK_Pay_Invoice FOREIGN KEY (invoice_id) REFERENCES Invoices(id),
    CONSTRAINT CK_Pay_amount  CHECK (amount > 0),
    CONSTRAINT CK_Pay_method  CHECK (method IN ('card','ach','wire','check','cash')),
    CONSTRAINT CK_Pay_curr    CHECK (currency = UPPER(currency))
);

CREATE TABLE Tags (
    id          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    company_id  INT               NOT NULL,
    name        NVARCHAR(50)      NOT NULL,
    color       CHAR(7)           NULL,
    created_at  DATETIME2         NOT NULL CONSTRAINT DF_Tag_created DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Tag_Company FOREIGN KEY (company_id) REFERENCES Companies(id),
    CONSTRAINT UQ_Tag_Name    UNIQUE (company_id, name),
    CONSTRAINT CK_Tag_color   CHECK (color IS NULL OR color LIKE '#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]')
);

CREATE TABLE CustomerTags (
    customer_id            INT       NOT NULL,
    tag_id                 INT       NOT NULL,
    tagged_at              DATETIME2 NOT NULL CONSTRAINT DF_CT_at DEFAULT GETUTCDATE(),
    tagged_by_employee_id  INT       NULL,
    CONSTRAINT PK_CustomerTags PRIMARY KEY (customer_id, tag_id),
    CONSTRAINT FK_CT_Customer  FOREIGN KEY (customer_id)           REFERENCES Customers(id) ON DELETE CASCADE,
    CONSTRAINT FK_CT_Tag       FOREIGN KEY (tag_id)                REFERENCES Tags(id)      ON DELETE CASCADE,
    CONSTRAINT FK_CT_Employee  FOREIGN KEY (tagged_by_employee_id) REFERENCES Employees(id)
);

-- ============================================================
-- crm_mysql.sql — 3NF CRM source schema, native MySQL/MariaDB idioms
--
-- Tested on:  MySQL 8.0+, MariaDB 10.6+
--
-- Engine-specific features exercised (for SMT migration testing):
--   * INT UNSIGNED AUTO_INCREMENT
--   * BIGINT UNSIGNED for high-volume tables
--   * ENUM type (very MySQL-flavored)
--   * SET type (multi-value enum, MySQL-specific)
--   * DATETIME(6) with microsecond precision
--   * TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
--     (auto-updating row timestamp — distinctive MySQL idiom)
--   * JSON column type
--   * GENERATED ... STORED / VIRTUAL computed columns
--   * TINYINT(1) for booleans
--   * TEXT, MEDIUMTEXT, LONGTEXT
--   * CHAR(36) for UUIDs (no native UUID type)
--   * Per-table ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE
--   * CHECK constraints (enforced in MySQL 8.0.16+, MariaDB 10.2.1+)
--   * Composite PK and composite FK to composite PK
--   * Self-referencing FKs
--   * CASCADE / SET NULL / NO ACTION mix
-- ============================================================

CREATE TABLE companies (
    id            INT UNSIGNED      NOT NULL AUTO_INCREMENT,
    code          VARCHAR(20)       NOT NULL,
    name          VARCHAR(200)      NOT NULL,
    industry      VARCHAR(50),
    founded_date  DATE,
    settings      JSON              NOT NULL,
    is_active     TINYINT(1)        NOT NULL DEFAULT 1,
    created_at    TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_companies_code (code),
    CONSTRAINT chk_companies_active CHECK (is_active IN (0, 1)),
    CONSTRAINT chk_companies_code_upper CHECK (code = UPPER(code))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE departments (
    id            INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    company_id    INT UNSIGNED  NOT NULL,
    parent_id     INT UNSIGNED  NULL,
    name          VARCHAR(150)  NOT NULL,
    code          VARCHAR(20)   NOT NULL,
    cost_center   VARCHAR(20),
    is_active     TINYINT(1)    NOT NULL DEFAULT 1,
    created_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_dept_code (company_id, code),
    KEY idx_dept_parent (parent_id),
    CONSTRAINT fk_dept_company FOREIGN KEY (company_id) REFERENCES companies(id),
    CONSTRAINT fk_dept_parent  FOREIGN KEY (parent_id)  REFERENCES departments(id),
    CONSTRAINT chk_dept_active   CHECK (is_active IN (0, 1)),
    CONSTRAINT chk_dept_code_up  CHECK (code = UPPER(code))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE employees (
    id                INT UNSIGNED          NOT NULL AUTO_INCREMENT,
    company_id        INT UNSIGNED          NOT NULL,
    department_id     INT UNSIGNED          NULL,
    manager_id        INT UNSIGNED          NULL,
    employee_uuid     CHAR(36)              NOT NULL DEFAULT (UUID()),
    employee_no       VARCHAR(20)           NOT NULL,
    first_name        VARCHAR(100)          NOT NULL,
    last_name         VARCHAR(100)          NOT NULL,
    full_name         VARCHAR(201)          GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
    email             VARCHAR(255)          NOT NULL,
    phone             VARCHAR(50),
    job_title         VARCHAR(150),
    salary            DECIMAL(12,2),
    hire_date         DATETIME(6)           NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    termination_date  DATETIME(6),
    employment_type   ENUM('full_time','part_time','contract','intern')
                                            NOT NULL DEFAULT 'full_time',
    skills            SET('sales','support','engineering','finance','hr','legal'),
    profile           JSON,
    is_active         TINYINT(1)            NOT NULL DEFAULT 1,
    created_at        TIMESTAMP             NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP             NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_emp_no    (company_id, employee_no),
    UNIQUE KEY uq_emp_email (email),
    UNIQUE KEY uq_emp_uuid  (employee_uuid),
    KEY idx_emp_dept    (department_id),
    KEY idx_emp_manager (manager_id),
    CONSTRAINT fk_emp_company FOREIGN KEY (company_id)    REFERENCES companies(id),
    CONSTRAINT fk_emp_dept    FOREIGN KEY (department_id) REFERENCES departments(id),
    CONSTRAINT fk_emp_manager FOREIGN KEY (manager_id)    REFERENCES employees(id),
    CONSTRAINT chk_emp_active CHECK (is_active IN (0, 1)),
    CONSTRAINT chk_emp_salary CHECK (salary IS NULL OR salary >= 0),
    CONSTRAINT chk_emp_term   CHECK (termination_date IS NULL OR termination_date >= hire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE customers (
    id                    INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    company_id            INT UNSIGNED  NOT NULL,
    external_id           CHAR(36)      NOT NULL DEFAULT (UUID()),
    customer_code         VARCHAR(20)   NOT NULL,
    customer_type         ENUM('individual','company','government')
                                        NOT NULL DEFAULT 'individual',
    company_name          VARCHAR(200),
    first_name            VARCHAR(100),
    last_name             VARCHAR(100),
    email                 VARCHAR(255)  NOT NULL,
    phone                 VARCHAR(50),
    fax                   VARCHAR(50),
    website               VARCHAR(500),
    tax_id                VARCHAR(50),
    credit_limit          DECIMAL(15,2) NOT NULL DEFAULT 0,
    notes                 MEDIUMTEXT,
    metadata              JSON,
    assigned_employee_id  INT UNSIGNED  NULL,
    is_active             TINYINT(1)    NOT NULL DEFAULT 1,
    is_deleted            TINYINT(1)    NOT NULL DEFAULT 0,
    deleted_at            DATETIME(6),
    created_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_cust_code     (company_id, customer_code),
    UNIQUE KEY uq_cust_external (external_id),
    UNIQUE KEY uq_cust_email    (email),
    KEY idx_cust_employee (assigned_employee_id),
    CONSTRAINT fk_cust_company  FOREIGN KEY (company_id)           REFERENCES companies(id),
    CONSTRAINT fk_cust_employee FOREIGN KEY (assigned_employee_id) REFERENCES employees(id) ON DELETE SET NULL,
    CONSTRAINT chk_cust_active   CHECK (is_active  IN (0, 1)),
    CONSTRAINT chk_cust_deleted  CHECK (is_deleted IN (0, 1)),
    CONSTRAINT chk_cust_credit   CHECK (credit_limit >= 0),
    CONSTRAINT chk_cust_identity CHECK (
        (customer_type = 'individual' AND first_name IS NOT NULL AND last_name IS NOT NULL)
     OR (customer_type IN ('company','government') AND company_name IS NOT NULL)
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE customer_addresses (
    id              INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    customer_id     INT UNSIGNED  NOT NULL,
    address_type    ENUM('billing','shipping','physical','mailing') NOT NULL,
    line1           VARCHAR(255)  NOT NULL,
    line2           VARCHAR(255),
    city            VARCHAR(100)  NOT NULL,
    state_province  VARCHAR(100),
    postal_code     VARCHAR(20),
    country_code    CHAR(2)       NOT NULL,
    is_primary      TINYINT(1)    NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    KEY idx_addr_customer (customer_id),
    CONSTRAINT fk_addr_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    CONSTRAINT chk_addr_primary CHECK (is_primary IN (0, 1)),
    CONSTRAINT chk_addr_country CHECK (country_code = UPPER(country_code))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE contacts (
    id           INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    customer_id  INT UNSIGNED  NOT NULL,
    first_name   VARCHAR(100)  NOT NULL,
    last_name    VARCHAR(100)  NOT NULL,
    title        VARCHAR(100),
    email        VARCHAR(255),
    phone        VARCHAR(50),
    is_primary   TINYINT(1)    NOT NULL DEFAULT 0,
    is_active    TINYINT(1)    NOT NULL DEFAULT 1,
    created_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_contact_customer (customer_id),
    CONSTRAINT fk_contact_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    CONSTRAINT chk_contact_primary CHECK (is_primary IN (0, 1)),
    CONSTRAINT chk_contact_active  CHECK (is_active  IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE product_categories (
    id          INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    parent_id   INT UNSIGNED  NULL,
    name        VARCHAR(150)  NOT NULL,
    slug        VARCHAR(100)  NOT NULL,
    sort_order  INT           NOT NULL DEFAULT 0,
    is_active   TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    UNIQUE KEY uq_pcat_slug (slug),
    KEY idx_pcat_parent (parent_id),
    CONSTRAINT fk_pcat_parent FOREIGN KEY (parent_id) REFERENCES product_categories(id),
    CONSTRAINT chk_pcat_active CHECK (is_active IN (0, 1)),
    CONSTRAINT chk_pcat_slug_lo CHECK (slug = LOWER(slug))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE products (
    id            INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    category_id   INT UNSIGNED  NULL,
    sku           VARCHAR(50)   NOT NULL,
    name          VARCHAR(255)  NOT NULL,
    description   TEXT,
    unit_price    DECIMAL(12,2) NOT NULL,
    cost_price    DECIMAL(12,2),
    currency      CHAR(3)       NOT NULL DEFAULT 'USD',
    weight_grams  INT UNSIGNED,
    margin        DECIMAL(8,4)  GENERATED ALWAYS AS (
        CASE WHEN cost_price IS NULL OR cost_price = 0 THEN NULL
             ELSE (unit_price - cost_price) / cost_price END
    ) VIRTUAL,
    attributes    JSON,
    is_taxable    TINYINT(1)    NOT NULL DEFAULT 1,
    is_active     TINYINT(1)    NOT NULL DEFAULT 1,
    created_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_prod_sku (sku),
    KEY idx_prod_category (category_id),
    CONSTRAINT fk_prod_category FOREIGN KEY (category_id) REFERENCES product_categories(id),
    CONSTRAINT chk_prod_price    CHECK (unit_price >= 0),
    CONSTRAINT chk_prod_cost     CHECK (cost_price IS NULL OR cost_price >= 0),
    CONSTRAINT chk_prod_currency CHECK (currency = UPPER(currency)),
    CONSTRAINT chk_prod_taxable  CHECK (is_taxable IN (0, 1)),
    CONSTRAINT chk_prod_active   CHECK (is_active  IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE orders (
    id             INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    company_id     INT UNSIGNED  NOT NULL,
    customer_id    INT UNSIGNED  NOT NULL,
    order_no       VARCHAR(30)   NOT NULL,
    order_date     DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    required_date  DATETIME(6),
    shipped_at     DATETIME(6),
    subtotal       DECIMAL(15,2) NOT NULL DEFAULT 0,
    tax_amount     DECIMAL(15,2) NOT NULL DEFAULT 0,
    total_amount   DECIMAL(15,2) NOT NULL DEFAULT 0,
    currency       CHAR(3)       NOT NULL DEFAULT 'USD',
    status         ENUM('pending','paid','shipped','delivered','cancelled')
                                 NOT NULL DEFAULT 'pending',
    sales_rep_id   INT UNSIGNED  NULL,
    notes          TEXT,
    metadata       JSON,
    created_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_order_no (company_id, order_no),
    KEY idx_order_customer (customer_id),
    KEY idx_order_rep (sales_rep_id),
    CONSTRAINT fk_order_company  FOREIGN KEY (company_id)   REFERENCES companies(id),
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id)  REFERENCES customers(id),
    CONSTRAINT fk_order_rep      FOREIGN KEY (sales_rep_id) REFERENCES employees(id) ON DELETE SET NULL,
    CONSTRAINT chk_order_required CHECK (required_date IS NULL OR required_date >= order_date),
    CONSTRAINT chk_order_amounts  CHECK (subtotal >= 0 AND tax_amount >= 0 AND total_amount >= 0),
    CONSTRAINT chk_order_total    CHECK (total_amount = subtotal + tax_amount),
    CONSTRAINT chk_order_curr     CHECK (currency = UPPER(currency))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE order_items (
    order_id      INT UNSIGNED  NOT NULL,
    line_no       INT UNSIGNED  NOT NULL,
    product_id    INT UNSIGNED  NOT NULL,
    quantity      DECIMAL(10,3) NOT NULL,
    unit_price    DECIMAL(12,2) NOT NULL,
    discount_pct  DECIMAL(5,2)  NOT NULL DEFAULT 0,
    line_total    DECIMAL(15,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount_pct / 100)) STORED,
    PRIMARY KEY (order_id, line_no),
    KEY idx_oi_product (product_id),
    CONSTRAINT fk_oi_order   FOREIGN KEY (order_id)   REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_oi_product FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT chk_oi_qty   CHECK (quantity > 0),
    CONSTRAINT chk_oi_price CHECK (unit_price >= 0),
    CONSTRAINT chk_oi_disc  CHECK (discount_pct BETWEEN 0 AND 100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE invoices (
    id            INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    order_id      INT UNSIGNED  NOT NULL,
    invoice_no    VARCHAR(30)   NOT NULL,
    issue_date    DATE          NOT NULL,
    due_date      DATE          NOT NULL,
    subtotal      DECIMAL(15,2) NOT NULL,
    tax_amount    DECIMAL(15,2) NOT NULL,
    total_amount  DECIMAL(15,2) NOT NULL,
    currency      CHAR(3)       NOT NULL DEFAULT 'USD',
    status        ENUM('draft','sent','paid','overdue','void') NOT NULL DEFAULT 'draft',
    sent_at       DATETIME(6),
    paid_at       DATETIME(6),
    notes         TEXT,
    created_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_inv_no (invoice_no),
    KEY idx_inv_order (order_id),
    CONSTRAINT fk_inv_order FOREIGN KEY (order_id) REFERENCES orders(id),
    CONSTRAINT chk_inv_due   CHECK (due_date >= issue_date),
    CONSTRAINT chk_inv_amts  CHECK (subtotal >= 0 AND tax_amount >= 0 AND total_amount >= 0),
    CONSTRAINT chk_inv_total CHECK (total_amount = subtotal + tax_amount),
    CONSTRAINT chk_inv_curr  CHECK (currency = UPPER(currency))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE payments (
    id            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    invoice_id    INT UNSIGNED     NOT NULL,
    amount        DECIMAL(15,2)    NOT NULL,
    currency      CHAR(3)          NOT NULL DEFAULT 'USD',
    method        ENUM('card','ach','wire','check','cash','crypto') NOT NULL,
    reference_no  VARCHAR(100),
    paid_at       DATETIME(6)      NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    notes         TEXT,
    PRIMARY KEY (id),
    KEY idx_pay_invoice (invoice_id),
    CONSTRAINT fk_pay_invoice FOREIGN KEY (invoice_id) REFERENCES invoices(id),
    CONSTRAINT chk_pay_amount CHECK (amount > 0),
    CONSTRAINT chk_pay_curr   CHECK (currency = UPPER(currency))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE tags (
    id          INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    company_id  INT UNSIGNED  NOT NULL,
    name        VARCHAR(50)   NOT NULL,
    color       CHAR(7),
    created_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tag_name (company_id, name),
    CONSTRAINT fk_tag_company FOREIGN KEY (company_id) REFERENCES companies(id),
    CONSTRAINT chk_tag_color  CHECK (color IS NULL OR color REGEXP '^#[0-9A-Fa-f]{6}$')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE customer_tags (
    customer_id            INT UNSIGNED   NOT NULL,
    tag_id                 INT UNSIGNED   NOT NULL,
    tagged_at              TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tagged_by_employee_id  INT UNSIGNED   NULL,
    PRIMARY KEY (customer_id, tag_id),
    KEY idx_ct_tag (tag_id),
    KEY idx_ct_employee (tagged_by_employee_id),
    CONSTRAINT fk_ct_customer FOREIGN KEY (customer_id)           REFERENCES customers(id) ON DELETE CASCADE,
    CONSTRAINT fk_ct_tag      FOREIGN KEY (tag_id)                REFERENCES tags(id)      ON DELETE CASCADE,
    CONSTRAINT fk_ct_employee FOREIGN KEY (tagged_by_employee_id) REFERENCES employees(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

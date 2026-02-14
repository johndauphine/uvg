# UVg

A Rust reimplementation of [sqlacodegen](https://github.com/agronholm/sqlacodegen) — connects to a PostgreSQL database, introspects its schema, and generates SQLAlchemy Python model code.

Single binary, drop-in compatible CLI, same output.

## Installation

```bash
cargo install --path .
```

## Usage

```
uvg <database-url>
```

Accepts SQLAlchemy-style URLs:

```bash
# Declarative ORM classes (default)
uvg postgresql://user:pass@localhost/mydb

# Table objects
uvg --generator tables postgresql://user:pass@localhost/mydb

# Filter specific tables
uvg --tables users,posts postgresql://user:pass@localhost/mydb

# Write to file
uvg --outfile models.py postgresql://user:pass@localhost/mydb
```

### Options

| Flag | Description |
|---|---|
| `--generator <TYPE>` | `declarative` (default) or `tables` |
| `--tables <LIST>` | Comma-delimited table names to include |
| `--schemas <LIST>` | Schemas to introspect (default: `public`) |
| `--noviews` | Skip views |
| `--options <LIST>` | `noindexes`, `noconstraints`, `nocomments`, `use_inflect`, `nojoined`, `nobidi` |
| `--outfile <PATH>` | Output file (default: stdout) |

## Output Examples

### Declarative generator

```python
from typing import Optional

import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    email: Mapped[str] = mapped_column(String(255), unique=True)
    bio: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), server_default=text('now()')
    )
```

### Tables generator

```python
from sqlalchemy import Column, Integer, MetaData, String, Table, Text

metadata = MetaData()

t_users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False),
    Column('email', Text),
    schema='public'
)
```

## Supported PostgreSQL Types

Scalars: `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval`

Dialect types: `uuid`, `json`, `jsonb`, `inet`, `cidr`

Arrays: `_int4`, `_text`, and other array types via the `ARRAY()` wrapper

## Building from source

```bash
git clone https://github.com/johndauphine/UVg.git
cd UVg
cargo build --release
```

## Running tests

```bash
cargo test
```

Integration tests require a live PostgreSQL database:

```bash
DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored
```

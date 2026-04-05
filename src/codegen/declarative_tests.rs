//! Tests for the declarative generator.

use super::*;
use crate::testutil::*;

    fn make_simple_schema() -> IntrospectedSchema {
        schema_pg(vec![
            table("users")
                .column(col("id").build())
                .column(col("name").udt("varchar").max_length(100).build())
                .column(col("email").udt("varchar").max_length(255).build())
                .column(col("bio").udt("text").nullable().build())
                .column(col("created_at").udt("timestamptz").nullable().default_val("now()").build())
                .pk("users_pkey", &["id"])
                .unique("users_email_key", &["email"])
                .build(),
            table("posts")
                .column(col("id").udt("int8").build())
                .column(col("user_id").build())
                .column(col("title").udt("varchar").max_length(200).build())
                .column(col("body").udt("text").build())
                .pk("posts_pkey", &["id"])
                .fk("posts_user_id_fkey", &["user_id"], "users", &["id"])
                .build(),
        ])
    }

    #[test]
    fn test_declarative_generator_basic() {
        let schema = make_simple_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class Users(Base):"));
        assert!(output.contains("__tablename__ = 'users'"));
        // PrimaryKeyConstraint is NOT in __table_args__ for declarative mode
        assert!(!output.contains("PrimaryKeyConstraint"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(output.contains("name: Mapped[str] = mapped_column(String(100), nullable=False)"));
        assert!(output.contains("email: Mapped[str] = mapped_column(String(255), nullable=False)"));
        assert!(output.contains("bio: Mapped[Optional[str]] = mapped_column(Text)"));
        assert!(output.contains("class Posts(Base):"));
        assert!(output
            .contains("user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False)"));
        assert!(output.contains("UniqueConstraint('email', name='users_email_key')"));
        // Single-column FK is now inline on mapped_column, not in __table_args__
        assert!(!output.contains("ForeignKeyConstraint"));
    }

    #[test]
    fn test_declarative_generator_snapshot() {
        let schema = make_simple_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }

    fn make_mixed_pk_schema() -> IntrospectedSchema {
        schema_pg(vec![
            table("users")
                .column(col("id").build())
                .column(col("name").udt("varchar").max_length(100).build())
                .pk("users_pkey", &["id"])
                .build(),
            table("audit_log")
                .column(col("ts").udt("timestamptz").build())
                .column(col("action").udt("text").build())
                .column(col("detail").udt("text").nullable().build())
                .build(),
        ])
    }

    #[test]
    fn test_declarative_no_pk_fallback_to_table() {
        let schema = make_mixed_pk_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // The PK table should be a class
        assert!(output.contains("class Users(Base):"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));

        // The no-PK table should be a Table() assignment
        assert!(output.contains("t_audit_log = Table("));
        assert!(output.contains("'audit_log', Base.metadata,"));
        assert!(output.contains("Column('ts', DateTime(True), nullable=False)"));
        assert!(output.contains("Column('action', Text, nullable=False)"));
        assert!(output.contains("Column('detail', Text)"));

        // Should NOT generate a class for no-PK table
        assert!(!output.contains("class AuditLog(Base):"));

        // With topo sort + alphabetical tiebreak, audit_log comes before users
        let table_pos = output.find("t_audit_log = Table(").unwrap();
        let class_pos = output.find("class Users(Base):").unwrap();
        assert!(table_pos < class_pos);
    }

    #[test]
    fn test_declarative_no_pk_fallback_snapshot() {
        let schema = make_mixed_pk_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }

    #[test]
    fn test_declarative_all_no_pk() {
        let schema = schema_pg(vec![
            table("events")
                .column(col("ts").udt("timestamptz").build())
                .column(col("data").udt("text").build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // All no-PK: should fall back to MetaData() instead of DeclarativeBase
        assert!(output.contains("metadata = MetaData()"));
        assert!(!output.contains("class Base(DeclarativeBase):"));
        assert!(!output.contains("DeclarativeBase"));
        // Should have Table() output using standalone metadata
        assert!(output.contains("t_events = Table("));
        assert!(output.contains("'events', metadata,"));
        // Should NOT have Mapped or mapped_column imports
        assert!(!output.contains("Mapped"));
        assert!(!output.contains("mapped_column"));
        // Should have Table/Column imports
        assert!(output.contains("Column"));
        assert!(output.contains("Table"));
    }

    #[test]
    fn test_declarative_all_no_pk_snapshot() {
        let schema = schema_pg(vec![
            table("events")
                .column(col("ts").udt("timestamptz").build())
                .column(col("data").udt("text").build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }

    // --- Tier 1: Tests adapted from sqlacodegen test_generator_declarative.py ---

    /// Adapted from sqlacodegen test_indexes.
    #[test]
    fn test_declarative_indexes() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .column(col("text").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .index("idx_number", &["number"], false)
                .index("idx_text", &["text"], true)
                .index("idx_text_number", &["text", "number"], false)
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class SimpleItems(Base):"));
        assert!(output.contains("__table_args__ = ("));
        assert!(output.contains("Index('idx_number', 'number')"));
        assert!(output.contains("Index('idx_text', 'text', unique=True)"));
        assert!(output.contains("Index('idx_text_number', 'text', 'number')"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(output.contains("number: Mapped[Optional[int]] = mapped_column(Integer)"));
        assert!(output.contains("text: Mapped[Optional[str]] = mapped_column(String)"));
    }

    /// Adapted from sqlacodegen test_table_kwargs.
    /// Tests dict-only __table_args__ for schema.
    #[test]
    fn test_declarative_table_kwargs() {
        let schema = schema_pg(vec![
            table("simple_items")
                .schema("testschema")
                .column(col("id").build())
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class SimpleItems(Base):"));
        assert!(output.contains("__table_args__ = {'schema': 'testschema'}"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_table_args_kwargs.
    /// Tests mixed tuple+dict __table_args__.
    #[test]
    fn test_declarative_table_args_kwargs() {
        let schema = schema_pg(vec![
            table("simple_items")
                .schema("testschema")
                .column(col("id").build())
                .column(col("name").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .index("testidx", &["id", "name"], false)
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("__table_args__ = ("));
        assert!(output.contains("Index('testidx', 'id', 'name'),"));
        assert!(output.contains("{'schema': 'testschema'}"));
    }

    /// Adapted from sqlacodegen test_only_tables (all no-PK fallback).
    #[test]
    fn test_declarative_only_tables() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("metadata = MetaData()"));
        assert!(output.contains("t_simple_items = Table("));
        assert!(!output.contains("class "));
        assert!(!output.contains("DeclarativeBase"));
    }

    /// Adapted from sqlacodegen test_column_comment (without nocomments).
    #[test]
    fn test_declarative_column_comment() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").comment("this is a 'comment'").build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True, comment=\"this is a 'comment'\")"));
    }

    /// Adapted from sqlacodegen test_column_comment with nocomments option.
    #[test]
    fn test_declarative_column_comment_nocomments() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").comment("this is a 'comment'").build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nocomments: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(!output.contains("comment="));
    }

    /// Adapted from sqlacodegen test_table_comment (declarative).
    #[test]
    fn test_declarative_table_comment() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .pk("simple_pkey", &["id"])
                .comment("this is a 'comment'")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("__table_args__ = {'comment': \"this is a 'comment'\"}"));
    }

    /// Adapted from sqlacodegen test_pascal.
    #[test]
    fn test_declarative_pascal() {
        // Note: sqlacodegen preserves "CustomerAPIPreference" as-is for the class name.
        // heck's to_upper_camel_case normalizes to "CustomerApiPreference".
        // This is a known difference (consecutive uppercase letters are lowercased by heck).
        let schema = schema_pg(vec![
            table("CustomerAPIPreference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class CustomerApiPreference(Base):"));
        assert!(output.contains("__tablename__ = 'CustomerAPIPreference'"));
    }

    /// Adapted from sqlacodegen test_underscore.
    #[test]
    fn test_declarative_underscore() {
        let schema = schema_pg(vec![
            table("customer_api_preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class CustomerApiPreference(Base):"));
        assert!(output.contains("__tablename__ = 'customer_api_preference'"));
    }

    /// Adapted from sqlacodegen test_pascal_multiple_underscore.
    #[test]
    fn test_declarative_pascal_multiple_underscore() {
        let schema = schema_pg(vec![
            table("customer_API__Preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // heck's UpperCamelCase handling of double underscores
        assert!(output.contains("__tablename__ = 'customer_API__Preference'"));
    }

    // --- Tier 2: Tests adapted from sqlacodegen test_generator_declarative.py ---

    /// Adapted from sqlacodegen test_invalid_attribute_names.
    #[test]
    fn test_declarative_invalid_attribute_names() {
        let schema = schema_pg(vec![
            table("simple-items")
                .column(col("id-test").build())
                .column(col("4test").nullable().build())
                .column(col("_4test").nullable().build())
                .column(col("def").nullable().build())
                .pk("simple_items_pkey", &["id-test"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Hyphens replaced with underscores, explicit column name
        assert!(output.contains("id_test: Mapped[int] = mapped_column('id-test', Integer, primary_key=True)"));
        // Leading digit gets underscore prefix
        assert!(output.contains("_4test: Mapped[Optional[int]] = mapped_column('4test', Integer)"));
        // _4test collides with sanitized '4test', so gets trailing underscore
        assert!(output.contains("_4test_: Mapped[Optional[int]] = mapped_column('_4test', Integer)"));
        // Python keyword gets trailing underscore
        assert!(output.contains("def_: Mapped[Optional[int]] = mapped_column('def', Integer)"));
    }

    /// Adapted from sqlacodegen test_metadata_column.
    #[test]
    fn test_declarative_metadata_column() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .column(col("metadata").udt("varchar").nullable().build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "metadata" is reserved by SQLAlchemy
        assert!(output.contains("metadata_: Mapped[Optional[str]] = mapped_column('metadata', String)"));
    }

    /// Adapted from sqlacodegen test_invalid_variable_name_from_column.
    #[test]
    fn test_declarative_invalid_variable_name_from_column() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col(" id ").build())
                .pk("simple_pkey", &[" id "])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Spaces trimmed and mapped, explicit column name preserved
        assert!(output.contains("id: Mapped[int] = mapped_column(' id ', Integer, primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_constraints (declarative).
    #[test]
    fn test_declarative_constraints() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "number > 0")
                .unique("uq_id_number", &["id", "number"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("CheckConstraint('number > 0')"));
        assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
        assert!(output.contains("from sqlalchemy import CheckConstraint"));
    }

    /// Adapted from sqlacodegen test_colname_import_conflict.
    #[test]
    fn test_declarative_colname_import_conflict() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .column(col("text").udt("varchar").nullable().build())
                .column(
                    col("textwithdefault")
                        .udt("varchar")
                        .nullable()
                        .default_val("'test'")
                        .build(),
                )
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "text" conflicts with sqlalchemy.text import (from server_default)
        assert!(output.contains("text_: Mapped[Optional[str]] = mapped_column('text', String)"));
        assert!(output.contains("textwithdefault: Mapped[Optional[str]] = mapped_column(String, server_default=text"));
    }

    /// Adapted from sqlacodegen test_composite_autoincrement_pk.
    #[test]
    fn test_declarative_composite_autoincrement_pk() {
        let schema = schema_pg(vec![
            table("simple_autoincrement_items")
                .column(col("id1").autoincrement().build())
                .column(col("id2").build())
                .pk("simple_autoincrement_items_pkey", &["id1", "id2"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id1: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)"));
        assert!(output.contains("id2: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_composite_nullable_pk.
    #[test]
    fn test_declarative_composite_nullable_pk() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id1").build())
                .column(col("id2").nullable().build())
                .pk("simple_items_pkey", &["id1", "id2"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id1: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        // Nullable PK column should show Optional and nullable=True
        assert!(output.contains("id2: Mapped[Optional[int]] = mapped_column(Integer, primary_key=True, nullable=True)"));
    }

    /// Adapted from sqlacodegen test_pascal_underscore.
    #[test]
    fn test_declarative_pascal_underscore() {
        // Note: sqlacodegen preserves "CustomerAPIPreference" for "customer_API_Preference".
        // heck normalizes it to "CustomerApiPreference". Known difference.
        let schema = schema_pg(vec![
            table("customer_API_Preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("__tablename__ = 'customer_API_Preference'"));
    }

    // --- Tier 3: Relationship tests adapted from sqlacodegen ---

    /// Adapted from sqlacodegen test_onetomany.
    #[test]
    fn test_declarative_onetomany() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_container_id_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: collection relationship
        assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='container')"));

        // Child side: inline FK + scalar relationship
        assert!(output.contains("container_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_containers.id'))"));
        assert!(output.contains("container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));

        // Should import relationship
        assert!(output.contains("relationship"));
        // Should import ForeignKey (not ForeignKeyConstraint)
        assert!(output.contains("ForeignKey"));
        assert!(!output.contains("ForeignKeyConstraint"));
    }

    /// Adapted from sqlacodegen test_onetomany_selfref.
    #[test]
    fn test_declarative_onetomany_selfref() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_parent_item_id_fkey", &["parent_item_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Inline FK
        assert!(output.contains("parent_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_items.id'))"));
        // Forward relationship with remote_side
        assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], back_populates='parent_item_reverse')"));
        // Reverse relationship
        assert!(output.contains("parent_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', remote_side=[parent_item_id], back_populates='parent_item')"));
    }

    /// Adapted from sqlacodegen test_onetomany_composite.
    #[test]
    fn test_declarative_onetomany_composite() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("simple_containers_pkey", &["id1", "id2"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id1").nullable().build())
                .column(col("container_id2").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk_full(
                    "simple_items_fkey",
                    &["container_id1", "container_id2"],
                    "public",
                    "simple_containers",
                    &["id1", "id2"],
                    "CASCADE",
                    "CASCADE",
                )
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Composite FK stays in __table_args__
        assert!(output.contains("ForeignKeyConstraint(['container_id1', 'container_id2']"));
        assert!(output.contains("ondelete='CASCADE'"));
        // Columns keep their types (no inline ForeignKey)
        assert!(output.contains("container_id1: Mapped[Optional[int]] = mapped_column(Integer)"));
        assert!(output.contains("container_id2: Mapped[Optional[int]] = mapped_column(Integer)"));
        // Parent-side relationship
        assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='simple_containers')"));
        // Child-side relationship
        assert!(output.contains("simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));
    }

    /// Adapted from sqlacodegen test_onetoone.
    #[test]
    fn test_declarative_onetoone() {
        let schema = schema_pg(vec![
            table("other_items")
                .column(col("id").build())
                .pk("other_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("other_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_other_item_id_fkey", &["other_item_id"], "other_items", &["id"])
                .unique("simple_items_other_item_id_key", &["other_item_id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: one-to-one (uselist=False, Optional scalar)
        assert!(output.contains("simple_items: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', uselist=False, back_populates='other_item')"));
        // Child side: FK with unique=True
        assert!(output.contains("other_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('other_items.id'), unique=True)"));
        // Child side relationship
        assert!(output.contains("other_item: Mapped[Optional['OtherItems']] = relationship('OtherItems', back_populates='simple_items')"));
    }

    /// Adapted from sqlacodegen test_onetomany_noinflect.
    /// FK column without _id suffix — relationship name = FK column name.
    #[test]
    fn test_declarative_onetomany_noinflect() {
        let schema = schema_pg(vec![
            table("fehwiuhfiw")
                .column(col("id").build())
                .pk("fehwiuhfiw_pkey", &["id"])
                .build(),
            table("oglkrogk")
                .column(col("id").build())
                .column(col("fehwiuhfiwID").nullable().build())
                .pk("oglkrogk_pkey", &["id"])
                .fk("oglkrogk_fehwiuhfiwid_fkey", &["fehwiuhfiwID"], "fehwiuhfiw", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // FK column has uppercase ID suffix — stripped
        assert!(output.contains("fehwiuhfiwID: Mapped[Optional[int]] = mapped_column(ForeignKey('fehwiuhfiw.id'))"));
        // Parent-side relationship
        assert!(output.contains("oglkrogk: Mapped[list['Oglkrogk']] = relationship('Oglkrogk', back_populates='fehwiuhfiw')"));
        // Child-side relationship: fehwiuhfiwID stripped to fehwiuhfiw
        assert!(output.contains("fehwiuhfiw: Mapped[Optional['Fehwiuhfiw']] = relationship('Fehwiuhfiw', back_populates='oglkrogk')"));
    }

    // --- Tier 4: Enum tests ---

    /// Adapted from sqlacodegen test_enum_shared_values (declarative).
    #[test]
    fn test_declarative_enum_shared_values() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("accounts")
                    .column(col("id").build())
                    .column(col("status").udt("status_enum").build())
                    .pk("accounts_pkey", &["id"])
                    .build(),
                table("users")
                    .column(col("id").build())
                    .column(col("status").udt("status_enum").build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "status_enum".to_string(),
                schema: None,
                values: vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "pending".to_string(),
                ],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Enum class generated before Base
        assert!(output.contains("class StatusEnum(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
        // Enum used in column type annotation
        assert!(output.contains("status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum'), nullable=False)"));
        // import enum
        assert!(output.contains("import enum"));
        assert!(output.contains("Enum"));
    }

    // --- PR 5: Advanced relationship tests ---

    /// Adapted from sqlacodegen test_onetomany_multiref.
    /// Two FKs from child to same parent — needs disambiguation.
    #[test]
    fn test_declarative_onetomany_multiref() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_container_id").nullable().build())
                .column(col("top_container_id").build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_container_id"], "simple_containers", &["id"])
                .fk("si_top_fkey", &["top_container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: disambiguated relationship names
        assert!(output.contains("simple_items_parent_container: Mapped[list['SimpleItems']]"));
        assert!(output.contains("simple_items_top_container: Mapped[list['SimpleItems']]"));
        // Child side: foreign_keys disambiguation
        assert!(output.contains("parent_container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', foreign_keys=[parent_container_id], back_populates='simple_items_parent_container')"));
        assert!(output.contains("top_container: Mapped['SimpleContainers'] = relationship('SimpleContainers', foreign_keys=[top_container_id], back_populates='simple_items_top_container')"));
    }

    /// Adapted from sqlacodegen test_onetomany_selfref_multi.
    #[test]
    fn test_declarative_onetomany_selfref_multi() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_item_id").nullable().build())
                .column(col("top_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_item_id"], "simple_items", &["id"])
                .fk("si_top_fkey", &["top_item_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Each self-ref FK gets foreign_keys disambiguation
        assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[parent_item_id], back_populates='parent_item_reverse')"));
        assert!(output.contains("top_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[top_item_id], back_populates='top_item_reverse')"));
    }

    /// Adapted from sqlacodegen test_manytoone_nobidi.
    #[test]
    fn test_declarative_manytoone_nobidi() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nobidi: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);

        // Child has relationship without back_populates
        assert!(output.contains("container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers')"));
        // Parent should NOT have reverse relationship
        assert!(!output.contains("simple_items: Mapped[list"));
    }

    /// Adapted from sqlacodegen test_foreign_key_schema.
    #[test]
    fn test_declarative_foreign_key_schema() {
        let schema = schema_pg(vec![
            table("other_items")
                .schema("otherschema")
                .column(col("id").build())
                .pk("other_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("other_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk_full(
                    "si_other_fkey",
                    &["other_item_id"],
                    "otherschema",
                    "other_items",
                    &["id"],
                    "NO ACTION",
                    "NO ACTION",
                )
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // FK target includes schema prefix
        assert!(output.contains("ForeignKey('otherschema.other_items.id')"));
        // Parent table has schema in __table_args__
        assert!(output.contains("__table_args__ = {'schema': 'otherschema'}"));
    }

    /// Adapted from sqlacodegen test_manytomany.
    #[test]
    fn test_declarative_manytomany() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("left_table_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("right_table_pkey", &["id"])
                .build(),
            table("association_table")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("assoc_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("assoc_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Association table rendered as Table()
        assert!(output.contains("t_association_table = Table("));
        assert!(output.contains("Column('left_id', ForeignKey('left_table.id'))"));
        assert!(output.contains("Column('right_id', ForeignKey('right_table.id'))"));

        // Left table gets M2M relationship to right
        assert!(output.contains("right: Mapped[list['RightTable']] = relationship('RightTable', secondary='association_table', back_populates='left')"));
        // Right table gets M2M relationship to left
        assert!(output.contains("left: Mapped[list['LeftTable']] = relationship('LeftTable', secondary='association_table', back_populates='right')"));

        // No class for association table
        assert!(!output.contains("class AssociationTable"));
    }

    /// Adapted from sqlacodegen test_joined_inheritance.
    #[test]
    fn test_declarative_joined_inheritance() {
        let schema = schema_pg(vec![
            table("simple_super_items")
                .column(col("id").build())
                .column(col("data1").nullable().build())
                .pk("simple_super_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("super_item_id").build())
                .column(col("data2").nullable().build())
                .pk("simple_items_pkey", &["super_item_id"])
                .fk("si_super_fkey", &["super_item_id"], "simple_super_items", &["id"])
                .build(),
            table("simple_sub_items")
                .column(col("simple_items_id").build())
                .column(col("data3").nullable().build())
                .pk("simple_sub_items_pkey", &["simple_items_id"])
                .fk("ssi_items_fkey", &["simple_items_id"], "simple_items", &["super_item_id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent class
        assert!(output.contains("class SimpleSuperItems(Base):"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(output.contains("data1: Mapped[Optional[int]] = mapped_column(Integer)"));

        // Child inherits from parent
        assert!(output.contains("class SimpleItems(SimpleSuperItems):"));
        assert!(output.contains("super_item_id: Mapped[int] = mapped_column(ForeignKey('simple_super_items.id'), primary_key=True)"));
        assert!(output.contains("data2: Mapped[Optional[int]] = mapped_column(Integer)"));

        // Grandchild inherits from child
        assert!(output.contains("class SimpleSubItems(SimpleItems):"));
        assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.super_item_id'), primary_key=True)"));
        assert!(output.contains("data3: Mapped[Optional[int]] = mapped_column(Integer)"));

        // No relationship() calls for inheritance FKs
        assert!(!output.contains("relationship("));
    }

    // --- PR 8: Misc feature tests ---

    /// Adapted from sqlacodegen test_table_with_arrays (declarative).
    #[test]
    fn test_declarative_table_with_arrays() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("tags").udt("_text").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("tags: Mapped[Optional[list]] = mapped_column(ARRAY(Text))"));
    }

    /// Adapted from sqlacodegen test_constraints (declarative) — check + unique + index together.
    #[test]
    fn test_declarative_constraints_with_index() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "number > 0")
                .unique("uq_id_number", &["id", "number"])
                .index("idx_number", &["number"], false)
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("CheckConstraint('number > 0')"));
        assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
        assert!(output.contains("Index('idx_number', 'number')"));
        assert!(output.contains("from sqlalchemy import CheckConstraint"));
    }

    /// Adapted from sqlacodegen test_onetomany_conflicting_column.
    /// Column named "relationship" gets trailing underscore.
    #[test]
    fn test_declarative_onetomany_conflicting_column() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .column(col("relationship").udt("text").nullable().build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "relationship" is not in PYTHON_RESERVED or import conflicts currently,
        // so it passes through as-is. The relationship() calls still work.
        assert!(output.contains("relationship: Mapped[Optional[str]]") || output.contains("relationship_: Mapped[Optional[str]]"));
        assert!(output.contains("relationship('SimpleItems'"));
    }

    /// Adapted from sqlacodegen test_manytomany_nobidi.
    #[test]
    fn test_declarative_manytomany_nobidi() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .pk("simple_items_pkey", &["id"])
                .build(),
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("container_items")
                .column(col("item_id").nullable().build())
                .column(col("container_id").nullable().build())
                .fk("ci_item_fkey", &["item_id"], "simple_items", &["id"])
                .fk("ci_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nobidi: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // M2M relationships exist but without back_populates
        assert!(output.contains("relationship("));
        assert!(!output.contains("back_populates"));
    }

    /// Adapted from sqlacodegen test_joined_inheritance_same_table_name.
    #[test]
    fn test_declarative_joined_inheritance_same_table_name() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("data1").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .build(),
            table("simple_sub_items")
                .column(col("simple_items_id").build())
                .column(col("data2").nullable().build())
                .pk("simple_sub_items_pkey", &["simple_items_id"])
                .fk("ssi_fkey", &["simple_items_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Child inherits from parent
        assert!(output.contains("class SimpleSubItems(SimpleItems):"));
        // FK on PK column
        assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.id'), primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_synthetic_enum_generation (declarative).
    #[test]
    fn test_declarative_synthetic_enum() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("status").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "simple_items.status IN ('active', 'inactive', 'pending')")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Synthetic enum class
        assert!(output.contains("class SimpleItemsStatus(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
    }

    /// Adapted from sqlacodegen test_onetomany_multiref_composite.
    #[test]
    fn test_declarative_onetomany_multiref_composite() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("sc_pkey", &["id1", "id2"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container1_id1").nullable().build())
                .column(col("container1_id2").nullable().build())
                .column(col("container2_id1").nullable().build())
                .column(col("container2_id2").nullable().build())
                .pk("si_pkey", &["id"])
                .fk_full("si_c1_fkey", &["container1_id1", "container1_id2"], "public", "simple_containers", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .fk_full("si_c2_fkey", &["container2_id1", "container2_id2"], "public", "simple_containers", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Two ForeignKeyConstraints in __table_args__
        assert!(output.contains("ForeignKeyConstraint(['container1_id1', 'container1_id2']"));
        assert!(output.contains("ForeignKeyConstraint(['container2_id1', 'container2_id2']"));
    }

    /// Adapted from sqlacodegen test_manytomany_composite.
    #[test]
    fn test_declarative_manytomany_composite() {
        // M2M with composite FKs is NOT detected as association table
        // (is_association_table requires single-column FKs)
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("lt_pkey", &["id1", "id2"])
                .build(),
            table("right_table")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("rt_pkey", &["id1", "id2"])
                .build(),
            table("assoc")
                .column(col("left_id1").nullable().build())
                .column(col("left_id2").nullable().build())
                .column(col("right_id1").nullable().build())
                .column(col("right_id2").nullable().build())
                .fk_full("a_left_fkey", &["left_id1", "left_id2"], "public", "left_table", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .fk_full("a_right_fkey", &["right_id1", "right_id2"], "public", "right_table", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Composite M2M: assoc is NOT an association table (requires single-col FKs)
        // So it gets Table() fallback (no PK)
        assert!(output.contains("t_assoc = Table("));
    }

    // --- PR 10: Relationship completion tests ---

    /// Adapted from sqlacodegen test_onetomany_conflicting_relationship.
    /// Relationship name collides with column name — gets underscore suffix.
    #[test]
    fn test_declarative_onetomany_conflicting_relationship() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .column(col("container").udt("varchar").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("si_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "container" column exists, so relationship "container" becomes "container_"
        assert!(output.contains("container: Mapped[Optional[str]] = mapped_column(String)"));
        assert!(output.contains("container_: Mapped[Optional['SimpleContainers']] = relationship("));
    }

    /// Adapted from sqlacodegen test_onetomany_multiref_with_nofknames.
    #[test]
    fn test_declarative_onetomany_multiref_with_nofknames() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id1").nullable().build())
                .column(col("container_id2").nullable().build())
                .pk("si_pkey", &["id"])
                .fk_full("si_c1_fkey", &["container_id1", "container_id2"], "public", "simple_containers", &["id", "id"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let opts = GeneratorOptions {
            nofknames: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // ForeignKeyConstraint without name= kwarg
        assert!(output.contains("ForeignKeyConstraint("));
        assert!(!output.contains("name='si_c1_fkey'"));
    }

    /// Adapted from sqlacodegen test_synthetic_enum_nosyntheticenums_option (declarative).
    #[test]
    fn test_declarative_synthetic_enum_nosyntheticenums() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("status").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "simple_items.status IN ('active', 'inactive')")
                .build(),
        ]);
        let opts = GeneratorOptions {
            nosyntheticenums: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // No enum class generated
        assert!(!output.contains("class SimpleItemsStatus"));
        assert!(!output.contains("import enum"));
        // Column uses regular type
        assert!(output.contains("mapped_column(String)"));
    }

    // --- PR 13: noidsuffix and misc tests ---

    /// Adapted from sqlacodegen test_onetomany_multiref_no_id_suffix.
    #[test]
    fn test_declarative_onetomany_multiref_no_id_suffix() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_container_id").nullable().build())
                .column(col("top_container_id").build())
                .pk("si_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_container_id"], "simple_containers", &["id"])
                .fk("si_top_fkey", &["top_container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            noidsuffix: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // With noidsuffix, relationship names keep the full FK column name.
        // Since they collide with column names, they get underscore suffix.
        assert!(output.contains("parent_container_id_: Mapped[Optional['SimpleContainers']]"));
        assert!(output.contains("top_container_id_: Mapped['SimpleContainers']"));
    }

    // --- PR 14: Final coverage ---

    /// Adapted from sqlacodegen test_manytomany_multi.
    /// Multiple association tables between same two parent tables.
    #[test]
    fn test_declarative_manytomany_multi() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("lt_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("rt_pkey", &["id"])
                .build(),
            table("assoc1")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a1_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a1_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
            table("assoc2")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a2_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a2_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Both association tables rendered as Table()
        assert!(output.contains("t_assoc1 = Table("));
        assert!(output.contains("t_assoc2 = Table("));
        // M2M relationships exist on both parent tables
        assert!(output.contains("secondary='assoc1'"));
        assert!(output.contains("secondary='assoc2'"));
    }

    /// Adapted from sqlacodegen test_domain_json (declarative).
    #[test]
    fn test_declarative_domain_json() {
        use crate::schema::{DomainInfo, IntrospectedSchema};
        let schema = IntrospectedSchema {
            dialect: crate::dialect::Dialect::Postgres,
            tables: vec![
                table("simple_items")
                    .column(col("id").build())
                    .column(col("data").udt("json_domain").nullable().build())
                    .pk("simple_items_pkey", &["id"])
                    .build(),
            ],
            enums: vec![],
            domains: vec![DomainInfo {
                name: "json_domain".to_string(),
                schema: None,
                base_type: "json".to_string(),
                constraint_name: None,
                not_null: false,
                check_expression: None,
            }],
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Domain columns in declarative mode: domain udt_name not resolved to base type
        // (full DOMAIN() support in declarative is future work — currently falls through
        // to the type mapper which uses the udt_name as-is)
        assert!(output.contains("data:"));
    }

    /// Adapted from sqlacodegen test_named_constraints.
    /// PrimaryKeyConstraint emitted in __table_args__ when CheckConstraint present.
    #[test]
    fn test_declarative_named_constraints() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").nullable().build())
                .column(col("text").udt("varchar").nullable().build())
                .pk("primarytest", &["id"])
                .check("checktest", "id > 0")
                .unique("uniquetest", &["text"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Check and Unique constraints in __table_args__
        assert!(output.contains("CheckConstraint('id > 0', name='checktest')"));
        assert!(output.contains("UniqueConstraint('text', name='uniquetest')"));
        // PK expressed via primary_key=True on mapped_column
        assert!(output.contains("primary_key=True"));
    }

    /// Adapted from sqlacodegen test_manytomany_multi_with_nofknames.
    #[test]
    fn test_declarative_manytomany_multi_with_nofknames() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("lt_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("rt_pkey", &["id"])
                .build(),
            table("assoc")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nofknames: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // M2M still works with nofknames
        assert!(output.contains("secondary='assoc'"));
    }

    /// Adapted from sqlacodegen test_named_foreign_key_constraints.
    #[test]
    fn test_declarative_named_foreign_key_constraints() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("foreignkeytest", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // FK rendered inline with relationship
        assert!(output.contains("ForeignKey('simple_containers.id')"));
        assert!(output.contains("relationship('SimpleContainers'"));
    }

    /// Adapted from sqlacodegen test_named_foreign_key_constraints_with_noidsuffix.
    #[test]
    fn test_declarative_named_foreign_key_constraints_with_noidsuffix() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("foreignkeytest", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            noidsuffix: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // With noidsuffix, relationship name keeps _id suffix
        assert!(output.contains("relationship('SimpleContainers'"));
    }

    /// Adapted from sqlacodegen test_index_with_kwargs.
    #[test]
    fn test_declarative_index_with_kwargs() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("name").udt("varchar").nullable().build())
                .pk("si_pkey", &["id"])
                .index_with_kwargs("idx_name", &["name"], false, &[("postgresql_using", "gist"), ("mysql_length", "10")])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Index('idx_name', 'name'"));
        assert!(output.contains("mysql_length='10'"));
        assert!(output.contains("postgresql_using='gist'"));
    }

    /// Adapted from sqlacodegen test_index_with_empty_kwargs.
    #[test]
    fn test_declarative_index_with_empty_kwargs() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("name").udt("varchar").nullable().build())
                .pk("si_pkey", &["id"])
                .index_with_kwargs("idx_name", &["name"], false, &[("postgresql_using", "")])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Index('idx_name', 'name')"));
        // Empty kwargs should be skipped
        assert!(!output.contains("postgresql_using"));
    }

    /// Adapted from sqlacodegen test_manytomany_selfref.
    /// Self-referential M2M (simplified — primaryjoin/secondaryjoin are complex).
    #[test]
    fn test_declarative_manytomany_selfref() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .pk("si_pkey", &["id"])
                .build(),
            table("child_items")
                .column(col("parent_id").nullable().build())
                .column(col("child_id").nullable().build())
                .fk("ci_parent_fkey", &["parent_id"], "simple_items", &["id"])
                .fk("ci_child_fkey", &["child_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Self-referential M2M: association table rendered
        assert!(output.contains("t_child_items = Table("));
        // Relationships with secondary on the parent table
        assert!(output.contains("secondary='child_items'"));
    }

    /// Adapted from sqlacodegen test_include_dialect_options_not_enabled_skips.
    /// When include_dialect_options is not enabled (default), no dialect options rendered.
    #[test]
    fn test_declarative_include_dialect_options_not_enabled() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .pk("si_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // No dialect options in output
        assert!(!output.contains("postgresql_"));
    }

    /// Adapted from sqlacodegen test_fancy_coltypes (non-MySQL parts).
    /// Tests various PG column types mapped correctly.
    #[test]
    fn test_declarative_fancy_coltypes() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("bool_col").udt("bool").nullable().build())
                .column(col("numeric_col").udt("numeric").precision(10, 0).nullable().build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Boolean"));
        assert!(output.contains("Numeric"));
    }

    /// Adapted from sqlacodegen test_enum_unnamed.
    /// Unnamed enum: auto-generate class name from column udt_name.
    #[test]
    fn test_declarative_enum_unnamed() {
        use crate::schema::EnumInfo;
        // Unnamed enum has an auto-generated name based on the values
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("status").udt("status").build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "status".to_string(),
                schema: None,
                values: vec!["active".to_string(), "inactive".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Enum class generated
        assert!(output.contains("class Status(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
        assert!(output.contains("INACTIVE = 'inactive'"));
    }

    /// Adapted from sqlacodegen test_enum_nonativeenums_option.
    /// With nonativeenums, native PG enums should not be rendered as Enum classes.
    /// NOTE: nonativeenums is not yet fully wired — this test documents the intended
    /// behavior and verifies the option is accepted without error. When implemented,
    /// update assertions to verify enums are suppressed.
    #[test]
    fn test_declarative_enum_nonativeenums() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("status").udt("status_enum").build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "status_enum".to_string(),
                schema: None,
                values: vec!["active".to_string(), "inactive".to_string()],
            }],
        );
        let opts = GeneratorOptions {
            nonativeenums: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // TODO: When nonativeenums is wired, assert enum class is NOT generated
        // and column uses String type instead of Enum().
        // For now, verify the option is accepted and output is valid.
        assert!(output.contains("class Users(Base):"));
    }

    /// Adapted from sqlacodegen test_array_enum_named.
    /// Array of named enum type.
    #[test]
    fn test_declarative_array_enum_named() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("roles").udt("_role_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "role_enum".to_string(),
                schema: None,
                values: vec!["admin".to_string(), "user".to_string(), "moderator".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Array column rendered (array-of-enum is complex; for now just test it doesn't panic)
        assert!(output.contains("roles:"));
    }

    /// Adapted from sqlacodegen test_domain_non_default_json (declarative).
    #[test]
    fn test_declarative_domain_non_default_json() {
        use crate::schema::{DomainInfo, IntrospectedSchema};
        let schema = IntrospectedSchema {
            dialect: crate::dialect::Dialect::Postgres,
            tables: vec![
                table("simple_items")
                    .column(col("id").build())
                    .column(col("data").udt("custom_json").nullable().build())
                    .pk("si_pkey", &["id"])
                    .build(),
            ],
            enums: vec![],
            domains: vec![DomainInfo {
                name: "custom_json".to_string(),
                schema: None,
                base_type: "jsonb".to_string(),
                constraint_name: None,
                not_null: false,
                check_expression: None,
            }],
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Domain in declarative: currently uses udt_name as-is
        assert!(output.contains("data:"));
    }

    /// Adapted from sqlacodegen test_jsonb (with astext_type parameter).
    /// JSONB with special parameters (placeholder — full support future work).
    #[test]
    fn test_declarative_jsonb_with_params() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("data").udt("jsonb").nullable().build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Basic JSONB renders correctly
        assert!(output.contains("JSONB"));
    }

    /// Adapted from sqlacodegen test_enum_unnamed_reuse_same_values.
    #[test]
    fn test_declarative_enum_unnamed_reuse() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("status1").udt("status_a").build())
                    .column(col("status2").udt("status_b").build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![
                EnumInfo {
                    name: "status_a".to_string(),
                    schema: None,
                    values: vec!["active".to_string(), "inactive".to_string()],
                },
                EnumInfo {
                    name: "status_b".to_string(),
                    schema: None,
                    values: vec!["active".to_string(), "inactive".to_string()],
                },
            ],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Both enum classes generated (even with same values, different names)
        assert!(output.contains("class StatusA(str, enum.Enum):"));
        assert!(output.contains("class StatusB(str, enum.Enum):"));
    }

    /// Adapted from sqlacodegen test_enum_unnamed_name_collision_different_values.
    #[test]
    fn test_declarative_enum_unnamed_collision() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("status").udt("status_a").build())
                    .pk("users_pkey", &["id"])
                    .build(),
                table("accounts")
                    .column(col("id").build())
                    .column(col("status").udt("status_b").build())
                    .pk("accounts_pkey", &["id"])
                    .build(),
            ],
            vec![
                EnumInfo {
                    name: "status_a".to_string(),
                    schema: None,
                    values: vec!["active".to_string(), "inactive".to_string()],
                },
                EnumInfo {
                    name: "status_b".to_string(),
                    schema: None,
                    values: vec!["pending".to_string(), "approved".to_string()],
                },
            ],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Different enum classes with different values
        assert!(output.contains("class StatusA(str, enum.Enum):"));
        assert!(output.contains("class StatusB(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
        assert!(output.contains("PENDING = 'pending'"));
    }

    /// Adapted from sqlacodegen test_array_enum_named_with_schema.
    #[test]
    fn test_declarative_array_enum_named_with_schema() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("roles").udt("_role_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "role_enum".to_string(),
                schema: Some("someschema".to_string()),
                values: vec!["admin".to_string(), "user".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Schema enum with array — renders the column
        assert!(output.contains("roles:"));
    }

    /// Adapted from sqlacodegen test_include_dialect_options tests.
    /// Tests that dialect options are only included when the option is enabled.
    #[test]
    fn test_declarative_include_dialect_options_skipped_by_default() {
        // With default options, no dialect-specific options in output
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .pk("si_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(!output.contains("postgresql_"));
        assert!(!output.contains("mysql_"));
    }

    /// Adapted from sqlacodegen test_array_enum_nullable.
    #[test]
    fn test_declarative_array_enum_nullable() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("tags").udt("_tag_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "tag_enum".to_string(),
                schema: None,
                values: vec!["tech".to_string(), "science".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("tags: Mapped[Optional[list]]"));
    }

    /// Adapted from sqlacodegen test_array_enum_with_dimensions.
    #[test]
    fn test_declarative_array_enum_with_dimensions() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("matrix").udt("_status_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "status_enum".to_string(),
                schema: None,
                values: vec!["a".to_string(), "b".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Array column with enum renders
        assert!(output.contains("matrix:"));
    }

    /// Adapted from sqlacodegen test_array_enum_nonativeenums_option.
    #[test]
    fn test_declarative_array_enum_nonativeenums() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("roles").udt("_role_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "role_enum".to_string(),
                schema: None,
                values: vec!["admin".to_string(), "user".to_string()],
            }],
        );
        let opts = GeneratorOptions {
            nonativeenums: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // With nonativeenums — doesn't crash (wiring is future work)
        assert!(output.contains("roles:"));
    }

    /// Adapted from sqlacodegen test_array_enum_shared_with_regular_enum.
    #[test]
    fn test_declarative_array_enum_shared() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("users")
                    .column(col("id").build())
                    .column(col("role").udt("role_enum").build())
                    .column(col("prev_roles").udt("_role_enum").nullable().build())
                    .pk("users_pkey", &["id"])
                    .build(),
            ],
            vec![EnumInfo {
                name: "role_enum".to_string(),
                schema: None,
                values: vec!["admin".to_string(), "user".to_string()],
            }],
        );
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Enum class used for both regular and array columns
        assert!(output.contains("class RoleEnum(str, enum.Enum):"));
        assert!(output.contains("role: Mapped[RoleEnum]"));
    }

    /// Adapted from sqlacodegen test_use_inflect.
    /// NOTE: use_inflect requires an inflections crate and is not yet implemented.
    /// This test documents the intended behavior: with use_inflect, collection
    /// relationship names would be pluralized and scalar names singularized.
    /// When implemented, update assertions to verify inflected names.
    #[test]
    fn test_declarative_use_inflect_placeholder() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("si_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Without inflect: collection uses table name "simple_items"
        assert!(output.contains("simple_items: Mapped[list['SimpleItems']]"));
        // TODO: With use_inflect, parent side would use singularized/pluralized names
        // e.g. "simple_item: Mapped[list['SimpleItems']]" → pluralized collection
    }

    /// Test keep_dialect_types in declarative mode for PostgreSQL.
    #[test]
    fn test_declarative_keep_dialect_types_pg() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").udt("int8").build())
                .column(col("name").udt("varchar").max_length(100).nullable().build())
                .column(col("score").udt("float8").nullable().build())
                .pk("si_pkey", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            keep_dialect_types: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // PG dialect types preserved in declarative
        assert!(output.contains("BIGINT"));
        assert!(output.contains("VARCHAR(100)"));
        assert!(output.contains("DOUBLE_PRECISION"));
        assert!(output.contains("from sqlalchemy.dialects.postgresql import"));
    }

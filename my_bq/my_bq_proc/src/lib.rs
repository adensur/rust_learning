extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput};

/*
r#"impl Deserialize for MyStruct {
        fn create_deserialize_indices(
            schema_fields: &Vec<TableFieldSchema>,
        ) -> Result<Decoder, BigQueryError> {
            let mut indices: Vec<usize> = vec![usize::MAX; 1];
            for (i, field) in schema_fields.iter().enumerate() {
                if field.name == "analytics_storage" {
                    if field.field_type != table_field_schema::Type::String {
                        return Err(BigQueryError::RowSchemaMismatch(format!(
                            "Expected String type for field analytics_storage, got {:?}",
                            field.field_type
                        )));
                    }
                    indices[0] = i;
                }
            }
            // check that all indices are filled
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    "Failed to find field 'analytics_storage' in schema".to_string(),
                ));
            }
            Ok(Decoder {
                indices,
                recursive_indices: Vec::new(),
            })
        }
        fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
            let analytics_storage_idx = decoder.indices[0];
            if row.fields.len() <= analytics_storage_idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: analytics_storage_idx + 1,
                    found: row.fields.len(),
                });
            }
            let analytics_storage = std::mem::take(&mut row.fields[analytics_storage_idx]);
            let analytics_storage = match analytics_storage.value {
                Some(Value::String(val)) => val,
                Some(other_value) => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected string value for field analytics_storage, found {:?}",
                        other_value
                    )))
                }
                None => {
                    return Err(BigQueryError::UnexpectedFieldType(format!(
                        "Expected required value for field analytics_storage, found null",
                    )))
                }
            };
            Ok(Self { analytics_storage })
        }
    }
*/

#[derive(Debug, Clone)]
enum SqlType {
    String,
    Integer,
    Option(Box<SqlType>),
}

impl SqlType {
    fn new(ty: &syn::Type) -> Self {
        match ty {
            syn::Type::Path(p) => {
                if p.path.segments.len() == 1 {
                    let id = p.path.segments[0].ident.clone();
                    if id == "String" {
                        SqlType::String
                    } else if id == "i64" {
                        SqlType::Integer
                    } else if id == "Option" {
                        let args = p.path.segments[0].arguments.clone();
                        if let syn::PathArguments::AngleBracketed(
                            syn::AngleBracketedGenericArguments { args, .. },
                        ) = args
                        {
                            if args.len() != 1 {
                                panic!("Only simple qualified types are supported, got: {:#?}", p);
                            }
                            match &args[0] {
                                syn::GenericArgument::Type(ty) => {
                                    let subtype = SqlType::new(ty);
                                    eprintln!("Got option with subtype: {:?}", subtype);
                                    return SqlType::Option(Box::new(subtype));
                                }
                                _ => panic!(
                                    "Only simple qualified types are supported, got: {:#?}",
                                    p
                                ),
                            }
                        } else {
                            panic!("Only simple qualified types are supported, got: {:#?}", p);
                        }
                    } else {
                        panic!("Unexpected type: {}", id)
                    }
                } else {
                    panic!("Only simple qualified types are supported, got: {:#?}", p)
                }
            }
            _ => panic!("Only simple qualified types are supported, got: {:#?}", ty),
        }
    }
}

struct Field {
    ty: syn::Type,
    sql_type: SqlType,
    ident: syn::Ident,
    name: String,
}

/*
fn is_string(ty: Type) -> bool {
    match ty {
        syn::Type::Path(p) => {
            if (p.path.segments.len() == 1 &&
        },
        _ => false,
    }
}*/

fn sql_type_to_parse_code(sql_type: &SqlType) -> proc_macro2::TokenStream {
    match sql_type {
        SqlType::String => {
            quote! {val}
        }
        SqlType::Integer => {
            quote! {val.parse()?}
        }
        _ => panic!("Only simple sql type is expected here!"),
    }
}

#[proc_macro_derive(Deserialize, attributes(my_bq))]
pub fn derive_deserialize_fn(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = ast.ident;
    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = ast.data
    {
        let mut fields = Vec::new();
        for field in named {
            let mut field_name: String = field.ident.clone().unwrap().to_string();
            for attr in &field.attrs {
                if !attr.path.is_ident("my_bq") {
                    continue;
                };

                if let Some(proc_macro2::TokenTree::Group(grp)) =
                    attr.tokens.clone().into_iter().next()
                {
                    let mut stream = grp.stream().into_iter();
                    match stream.next().unwrap() {
                        proc_macro2::TokenTree::Ident(i) => {
                            assert_eq!(i, "rename");
                        }
                        something_else => {
                            panic!("Expected 'rename' attribute, found {}", something_else)
                        }
                    }
                    match stream.next().unwrap() {
                        proc_macro2::TokenTree::Punct(p) => {
                            assert_eq!(p.as_char(), '=');
                        }
                        something_else => {
                            panic!(
                                "Expected '=' punctuation in rename attribute, found {}",
                                something_else
                            )
                        }
                    }
                    match stream.next().unwrap() {
                        proc_macro2::TokenTree::Literal(l) => {
                            field_name = l.to_string();
                            // strip quotes
                            field_name = field_name[1..field_name.len() - 1].to_string();
                        }
                        something_else => {
                            panic!("Expected name for the field, found {}", something_else)
                        }
                    }
                }
            }
            fields.push(Field {
                ty: field.ty.clone(),
                sql_type: SqlType::new(&field.ty),
                ident: field.ident.clone().unwrap(),
                name: field_name.to_string(),
            });
        }
        fields
    } else {
        panic!("Only structs with named fields are supported")
    };
    let fields_code1 = fields.iter().enumerate().map(|(i, f)| {
        let field_name = f.name.clone();
        let error = format!(
            "Expected {:?} type for field '{}', got {{:?}}",
            f.sql_type, field_name
        );
        let expected_sql_type = match f.sql_type.clone() {
            SqlType::String => quote! {table_field_schema::Type::String},
            SqlType::Integer => quote! {table_field_schema::Type::Integer},
            SqlType::Option(subtype) => match *subtype {
                SqlType::String => quote! {table_field_schema::Type::String},
                SqlType::Integer => quote! {table_field_schema::Type::Integer},
                _ => panic!("Unexpected subtype: {:?}", subtype),
            },
        };
        quote! {
            if field.name == #field_name {
                if field.field_type != #expected_sql_type {
                    return Err(BigQueryError::RowSchemaMismatch(format!(
                        #error, field.field_type
                    )));
                }
                indices[#i] = i;
            }
        }
    });
    let fields_code2 = fields.iter().enumerate().map(|(i, f)| {
        let error = format!("Failed to find field '{}' in schema", f.name);
        quote! {
            if indices[#i] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    #error.to_string()
                ));
            }
        }
    });
    let fields_len = fields.len();
    let indices_code = quote! {
        let mut indices: Vec<usize> = vec![usize::MAX; #fields_len];
        for (i, field) in schema_fields.iter().enumerate() {
            #(#fields_code1)*
        }
        // check that all indices are filled
        #(#fields_code2)*
        Ok(Decoder {
            indices,
            recursive_indices: Vec::new(),
        })
    };

    let fields_code3 = fields.iter().enumerate().map(|(i, f)| {
        let field_ident = f.ident.clone();
        let field_name = f.name.clone();
        let field_name_literal = format!("{}", field_name);
        let error1 = format!(
            "Expected string value for field {}, found {{:?}}",
            field_name_literal
        );
        let error2 = format!(
            "Expected string value for field {}, found null",
            field_name_literal
        );
        match &f.sql_type {
            SqlType::String | SqlType::Integer => {
                let parse_code = sql_type_to_parse_code(&f.sql_type);
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let #field_ident = std::mem::take(&mut row.fields[idx]);
                    let #field_ident = match #field_ident.value {
                        Some(Value::String(val)) => #parse_code,
                        Some(other_value) => {
                            return Err(BigQueryError::UnexpectedFieldType(format!(
                                #error1,
                                other_value
                            )))
                        }
                        None => {
                            return Err(BigQueryError::UnexpectedFieldType(
                                #error2.into()
                            ))
                        }
                    };
                }
            }
            SqlType::Option(subtype) => {
                let parse_code = sql_type_to_parse_code(&subtype);
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let #field_ident = std::mem::take(&mut row.fields[idx]);
                    let #field_ident = match #field_ident.value {
                        Some(Value::String(val)) => Some(#parse_code),
                        None => None,
                        Some(other_value) => {
                            return Err(BigQueryError::UnexpectedFieldType(format!(
                                #error1,
                                other_value
                            )))
                        }
                    };
                }
            }
        }
    });
    let field_names = fields.iter().map(|f| f.ident.clone());
    let deserialize_code = quote! {
        #(#fields_code3)*
        Ok(Self { #(#field_names,)* })
    };

    quote! {
        impl Deserialize for #ident {
            fn create_deserialize_indices(
                schema_fields: &Vec<TableFieldSchema>,
            ) -> Result<Decoder, BigQueryError> {
                #indices_code
            }
            fn deserialize(mut row: TableRow, decoder: &Decoder) -> Result<Self, BigQueryError> {
                #deserialize_code
            }
        }
    }
    .into()
}

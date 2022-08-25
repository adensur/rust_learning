extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[derive(Debug, Clone, PartialEq)]
enum SqlType {
    String,
    Integer,
    Float,
    Record,
    Option(Box<SqlType>),
    Repeated(Box<SqlType>),
}

struct Field {
    ty: syn::Type,
    inner_ty: syn::Type,
    sql_type: SqlType,
    ident: syn::Ident,
    name: String,
}

fn parse_type(ty: &syn::Type) -> (syn::Type, SqlType) {
    match ty {
        syn::Type::Path(p) => {
            if p.path.segments.len() == 1 {
                let id = p.path.segments[0].ident.clone();
                if id == "String" {
                    (ty.clone(), SqlType::String)
                } else if id == "i64" {
                    (ty.clone(), SqlType::Integer)
                } else if id == "f64" {
                    (ty.clone(), SqlType::Float)
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
                                let (inner_ty, inner_sql_ty) = parse_type(ty);
                                (inner_ty.clone(), SqlType::Option(Box::new(inner_sql_ty)))
                            }
                            _ => panic!("Only simple qualified types are supported, got: {:#?}", p),
                        }
                    } else {
                        panic!("Only simple qualified types are supported, got: {:#?}", p);
                    }
                } else if id == "Vec" {
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
                                let (inner_ty, inner_sql_ty) = parse_type(ty);
                                (inner_ty, SqlType::Repeated(Box::new(inner_sql_ty)))
                            }
                            _ => panic!("Only simple qualified types are supported, got: {:#?}", p),
                        }
                    } else {
                        panic!("Only simple qualified types are supported, got: {:#?}", p);
                    }
                } else {
                    // Some user custom type
                    (ty.clone(), SqlType::Record)
                }
            } else {
                panic!("Only simple qualified types are supported, got: {:#?}", p)
            }
        }
        _ => panic!("Only simple qualified types are supported, got: {:#?}", ty),
    }
}

impl Field {
    fn new(field: &syn::Field) -> Self {
        let field_name = if let Some(field_name) = field_name_from_attributes(&field.attrs) {
            field_name
        } else {
            field.ident.clone().unwrap().to_string()
        };
        let (inner_ty, sql_type) = parse_type(&field.ty);
        Field {
            ty: field.ty.clone(),
            inner_ty,
            sql_type,
            ident: field.ident.clone().unwrap(),
            name: field_name.to_string(),
        }
    }
}

fn sql_type_to_parse_code(sql_type: &SqlType) -> proc_macro2::TokenStream {
    match sql_type {
        SqlType::String => {
            quote! {val}
        }
        SqlType::Integer | SqlType::Float => {
            quote! {val.parse()?}
        }
        _ => panic!("Only simple sql type is expected here!"),
    }
}

fn field_name_from_attributes(attrs: &Vec<syn::Attribute>) -> Option<String> {
    for attr in attrs {
        if !attr.path.is_ident("my_bq") {
            continue;
        };

        if let Some(proc_macro2::TokenTree::Group(grp)) = attr.tokens.clone().into_iter().next() {
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
                    let mut field_name = l.to_string();
                    // strip quotes
                    field_name = field_name[1..field_name.len() - 1].to_string();
                    return Some(field_name);
                }
                something_else => {
                    panic!("Expected name for the field, found {}", something_else)
                }
            }
        }
    }
    None
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
            fields.push(Field::new(&field));
        }
        fields
    } else {
        panic!("Only structs with named fields are supported")
    };
    let mut recursive_idx = -1;
    let fields_code1 = fields.iter().enumerate().map(|(i, f)| {
        let field_name = f.name.clone();
        let error = format!(
            "Expected {:?} type for field '{}', got {{:?}}",
            f.sql_type, field_name
        );
        let expected_sql_type = match f.sql_type.clone() {
            SqlType::String => quote! {::my_bq::structs::table_field_schema::Type::String},
            SqlType::Integer => quote! {::my_bq::structs::table_field_schema::Type::Integer},
            SqlType::Float => quote! {::my_bq::structs::table_field_schema::Type::Float},
            SqlType::Record => quote! {::my_bq::structs::table_field_schema::Type::Record},
            SqlType::Option(subtype) => match *subtype {
                SqlType::String => quote! {::my_bq::structs::table_field_schema::Type::String},
                SqlType::Integer => quote! {::my_bq::structs::table_field_schema::Type::Integer},
                SqlType::Float => quote! {::my_bq::structs::table_field_schema::Type::Float},
                SqlType::Record => quote! {::my_bq::structs::table_field_schema::Type::Record},
                _ => panic!("Unexpected subtype: {:?}", subtype),
            },
            SqlType::Repeated(subtype) => match *subtype {
                SqlType::String => quote! {::my_bq::structs::table_field_schema::Type::String},
                SqlType::Integer => quote! {::my_bq::structs::table_field_schema::Type::Integer},
                SqlType::Float => quote! {::my_bq::structs::table_field_schema::Type::Float},
                SqlType::Record => quote! {::my_bq::structs::table_field_schema::Type::Record},
                _ => panic!("Unexpected subtype: {:?}", subtype),
            },
        };
        let repeated_check = if let SqlType::Repeated(_) = f.sql_type.clone() {
            let repeated_check_error = format!("Expected Repeated mode for field {}, got {{:?}}", f.name);
            quote!{
                if field.mode != ::my_bq::structs::table_field_schema::Mode::Repeated {
                    return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(format!(
                        #repeated_check_error,
                        field.mode
                    )));
                }
            }
        } else {
            quote!{}
        };
        let inner_type = f.inner_ty.clone();
        match &f.sql_type {
            SqlType::Record => {
                recursive_idx += 1;
                let recursive_error =
                    format!("Failed to find recursive schema for field {}", field_name);
                quote! {
                    if field.name == #field_name {
                        if field.field_type != #expected_sql_type {
                            return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(format!(
                                #error, field.field_type
                            )));
                        }
                        #repeated_check
                        match &field.fields {
                            Some(fields) => {
                                let decoder = #inner_type::create_deserialize_indices(&fields)?;
                                indices[#i] = i;
                                recursive_indices[#recursive_idx as usize] = Box::new(decoder);
                            }
                            None => {
                                return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(#recursive_error.to_string()));
                            }
                        }
                    }
                }
            },
            SqlType::Repeated(inner) if **inner == SqlType::Record => {
                recursive_idx += 1;
                let recursive_error =
                    format!("Failed to find recursive schema for field {}", field_name);
                quote! {
                    if field.name == #field_name {
                        if field.field_type != #expected_sql_type {
                            return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(format!(
                                #error, field.field_type
                            )));
                        }
                        #repeated_check
                        match &field.fields {
                            Some(fields) => {
                                let decoder = #inner_type::create_deserialize_indices(&fields)?;
                                indices[#i] = i;
                                recursive_indices[#recursive_idx as usize] = Box::new(decoder);
                            }
                            None => {
                                return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(#recursive_error.to_string()));
                            }
                        }
                    }
                }
            }
            _ => quote! {
                if field.name == #field_name {
                    if field.field_type != #expected_sql_type {
                        return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(format!(
                            #error, field.field_type
                        )));
                    }
                    #repeated_check
                    indices[#i] = i;
                }
            }
        }
    });
    let fields_code2 = fields.iter().enumerate().map(|(i, f)| {
        let error = format!("Failed to find field '{}' in schema", f.name);
        quote! {
            if indices[#i] == usize::MAX {
                return Err(::my_bq::error::BigQueryError::RowSchemaMismatch(
                    #error.to_string()
                ));
            }
        }
    });
    let fields_len = fields.len();
    let indices_code = quote! {
        let mut indices: Vec<usize> = vec![usize::MAX; #fields_len];
        let mut recursive_indices: Vec<Box<::my_bq::client::Decoder>> = Vec::new();
        for i in 0..#fields_len {
            recursive_indices.push(Box::new(::my_bq::client::Decoder::default()));
        }
        for (i, field) in schema_fields.iter().enumerate() {
            #(#fields_code1)*
        }
        // check that all indices are filled
        #(#fields_code2)*
        Ok(::my_bq::client::Decoder {
            indices,
            recursive_indices,
        })
    };

    let mut recursive_idx = -1;
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
            SqlType::String | SqlType::Integer | SqlType::Float => {
                let parse_code = sql_type_to_parse_code(&f.sql_type);
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(::my_bq::error::BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let #field_ident = std::mem::take(&mut row.fields[idx]);
                    let #field_ident = match #field_ident.value {
                        Some(my_bq::structs::row_field::Value::String(val)) => #parse_code,
                        Some(other_value) => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #error1,
                                other_value
                            )))
                        }
                        None => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(
                                #error2.into()
                            ))
                        }
                    };
                }
            }
            SqlType::Record => {
                recursive_idx += 1;
                let null_error =
                    format!("Expected required value for field {}, found null", f.name);
                let type_error =
                    format!("Expected Record value for field {}, found {{:?}}", f.name);
                let recursive_type = &f.inner_ty;
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(::my_bq::error::BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let #field_ident = std::mem::take(&mut row.fields[idx]);
                    let #field_ident = match #field_ident.value {
                        Some(my_bq::structs::row_field::Value::Record(val)) => {
                            #recursive_type::deserialize(val, &decoder.recursive_indices[#recursive_idx as usize])?
                        }
                        None => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #null_error,
                            )))
                        }
                        Some(other_value) => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #type_error,
                                other_value
                            )))
                        }
                    };
                }
            }
            SqlType::Option(subtype) => {
                let parse_code = sql_type_to_parse_code(&subtype);
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(::my_bq::error::BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let #field_ident = std::mem::take(&mut row.fields[idx]);
                    let #field_ident = match #field_ident.value {
                        Some(my_bq::structs::row_field::Value::String(val)) => Some(#parse_code),
                        None => None,
                        Some(other_value) => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #error1,
                                other_value
                            )))
                        }
                    };
                }
            }
            SqlType::Repeated(subtype) if **subtype == SqlType::Record => {
                recursive_idx += 1;
                let record_type = f.ty.clone();
                let record_inner_type = f.inner_ty.clone();
                let error1 = format!("Expected record value for items within field {}, found {{:?}}", f.name);
                let error2 = format!("Expected string value for field {}, found {{:?}}", f.name);
                let error3 = format!("Expected required value for field {}, found null", f.name);
                quote! {
                    let idx = decoder.indices[#i];
                    if row.fields.len() <= idx {
                        return Err(::my_bq::error::BigQueryError::NotEnoughFields {
                            expected: idx + 1,
                            found: row.fields.len(),
                        });
                    }
                    let mut #field_ident: #record_type = Vec::new();
                    let params = std::mem::take(&mut row.fields[idx]);
                    match params.value {
                        Some(my_bq::structs::row_field::Value::Array(values)) => {
                            for val in values {
                                match val.value {
                                    Some(my_bq::structs::row_field::Value::Record(val)) => {
                                        #field_ident.push(#record_inner_type::deserialize(
                                            val,
                                            &decoder.recursive_indices[#recursive_idx as usize],
                                        )?);
                                    }
                                    other_value => {
                                        return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                            #error1,
                                            other_value
                                        )))
                                    }
                                }
                            }
                        }
                        Some(other_value) => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #error2,
                                other_value
                            )))
                        }
                        None => {
                            return Err(::my_bq::error::BigQueryError::UnexpectedFieldType(format!(
                                #error3,
                            )))
                        }
                    };
                }
            },
            SqlType::Repeated(_) => panic!("")
        }
    });
    let field_names = fields.iter().map(|f| f.ident.clone());
    let deserialize_code = quote! {
        #(#fields_code3)*
        Ok(Self { #(#field_names,)* })
    };

    let res = quote! {
        impl ::my_bq::client::Deserialize for #ident {
            fn create_deserialize_indices(
                schema_fields: &Vec<::my_bq::structs::table_field_schema::TableFieldSchema>,
            ) -> Result<::my_bq::client::Decoder, ::my_bq::error::BigQueryError> {
                #indices_code
            }
            fn deserialize(mut row: ::my_bq::TableRow, decoder: &::my_bq::client::Decoder) -> Result<Self, ::my_bq::error::BigQueryError> {
                #deserialize_code
            }
        }
    }
    .into();
    res
}

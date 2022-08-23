extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Type};

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

#[proc_macro_derive(Deserialize)]
pub fn derive_deserialize_fn(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = ast.ident;
    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = ast.data
    {
        named
    } else {
        panic!("Only structs with named fields are supported")
    };

    let fields_code1 = fields.iter().enumerate().map(|(i, f)| {
        let field_name = f.ident.clone().unwrap();
        let field_name_literal = format!("{}", field_name);
        let ty = match &f.ty {
            Type::Path(ref p) => &p.path.segments[0].ident,
            _ => panic!("..."),
        };
        let error1 = format!(
            "Expected {} type for field {}, got {{:?}}",
            ty, field_name_literal
        );
        quote! {
            if field.name == #field_name_literal {
                if field.field_type != table_field_schema::Type::String {
                    return Err(BigQueryError::RowSchemaMismatch(format!(
                        #error1, field.field_type
                    )));
                }
                indices[#i] = i;
            }
        }
    });
    let fields_code2 = fields.iter().enumerate().map(|(i, f)| {
        let field_name_literal = format!("{}", f.ident.clone().unwrap());
        quote! {
            if indices[0] == usize::MAX {
                return Err(BigQueryError::RowSchemaMismatch(
                    //"Failed to find field '#field_name' in schema".to_string(),
                    #field_name_literal.into()
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
        let field_name = f.ident.clone().unwrap();
        let field_name_literal = format!("{}", field_name);
        let error1 = format!(
            "Expected string value for field {}, found {{:?}}",
            field_name_literal
        );
        let error2 = format!(
            "Expected string value for field {}, found null",
            field_name_literal
        );
        quote! {
            let idx = decoder.indices[#i];
            if row.fields.len() <= idx {
                return Err(BigQueryError::NotEnoughFields {
                    expected: idx + 1,
                    found: row.fields.len(),
                });
            }
            let #field_name = std::mem::take(&mut row.fields[idx]);
            let #field_name = match #field_name.value {
                Some(Value::String(val)) => val,
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
    });
    let field_names = fields.iter().map(|f| f.ident.clone().unwrap());
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

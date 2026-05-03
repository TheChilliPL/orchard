use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Fields, Ident, Item, LitStr, Token, Variant, parse::Parser, parse_macro_input,
    punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn short_names(args: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as Item);
    let args = proc_macro2::TokenStream::from(args);

    match item {
        Item::Struct(mut item) => {
            if !args.is_empty() {
                return syn::Error::new_spanned(
                    args,
                    "expected no arguments on struct-level short_names; use #[short_name(\"short\")] or #[short_name(\"short\", \"long\")] on fields",
                )
                .to_compile_error()
                .into();
            }

            if let Err(err) = rewrite_fields(&mut item.fields) {
                return err.to_compile_error().into();
            }

            quote!(#item).into()
        }
        Item::Enum(mut item) => {
            if !args.is_empty() {
                match parse_item_args_stream(args) {
                    Ok((short, long)) => {
                        item.attrs.push(syn::parse_quote! {
                            #[cfg_attr(feature = "long_json", serde(tag = #long))]
                        });
                        item.attrs.push(syn::parse_quote! {
                            #[cfg_attr(not(feature = "long_json"), serde(tag = #short))]
                        });
                    }
                    Err(err) => return err.to_compile_error().into(),
                }
            }

            for variant in &mut item.variants {
                if let Err(err) = rewrite_variant(variant) {
                    return err.to_compile_error().into();
                }
            }

            quote!(#item).into()
        }
        other => syn::Error::new_spanned(other, "#[short_names] only supports structs and enums")
            .to_compile_error()
            .into(),
    }
}

fn parse_item_args_stream(args: proc_macro2::TokenStream) -> syn::Result<(String, String)> {
    let parser = Punctuated::<LitStr, Token![,]>::parse_terminated;
    parse_item_lits(parser.parse2(args)?)
}

fn parse_item_lits(args: Punctuated<LitStr, Token![,]>) -> syn::Result<(String, String)> {
    if args.len() != 2 {
        return Err(syn::Error::new_spanned(
            quote! { #args },
            "expected two arguments: short_names(\"short\", \"long\")",
        ));
    }

    Ok((args[0].value(), args[1].value()))
}

fn parse_rename_args(attr: &Attribute, ident: Option<&Ident>) -> syn::Result<(String, String)> {
    let parser = Punctuated::<LitStr, Token![,]>::parse_terminated;
    let args = attr.parse_args_with(parser)?;

    match args.len() {
        1 => {
            let long = ident.ok_or_else(|| {
                syn::Error::new_spanned(
                    attr,
                    "short_name(\"short\") can only be used on named fields",
                )
            })?;
            Ok((args[0].value(), long.to_string()))
        }
        2 => Ok((args[0].value(), args[1].value())),
        _ => Err(syn::Error::new_spanned(
            quote! { #args },
            "expected short_name(\"short\") or short_name(\"short\", \"long\")",
        )),
    }
}

fn rewrite_variant(variant: &mut Variant) -> syn::Result<()> {
    variant.attrs = rewrite_attrs(
        std::mem::take(&mut variant.attrs),
        RenameTarget::Rename { ident: Some(&variant.ident) },
    )?;
    rewrite_fields(&mut variant.fields)
}

fn rewrite_fields(fields: &mut Fields) -> syn::Result<()> {
    match fields {
        Fields::Named(fields) => {
            for field in &mut fields.named {
                field.attrs = rewrite_attrs(
                    std::mem::take(&mut field.attrs),
                    RenameTarget::Rename { ident: field.ident.as_ref() },
                )?;
            }
        }
        Fields::Unnamed(fields) => {
            for field in &mut fields.unnamed {
                field.attrs = rewrite_attrs(
                    std::mem::take(&mut field.attrs),
                    RenameTarget::Rename { ident: None },
                )?;
            }
        }
        Fields::Unit => {}
    }

    Ok(())
}

enum RenameTarget<'a> {
    Rename { ident: Option<&'a Ident> },
}

fn rewrite_attrs(attrs: Vec<Attribute>, target: RenameTarget<'_>) -> syn::Result<Vec<Attribute>> {
    let mut rewritten_attrs = Vec::with_capacity(attrs.len());

    for attr in attrs {
        if attr.path().is_ident("short_name") {
            let RenameTarget::Rename { ident } = target;
            let (short, long) = parse_rename_args(&attr, ident)?;
            rewritten_attrs.push(syn::parse_quote! {
                #[cfg_attr(feature = "long_json", serde(rename = #long, alias = #short))]
            });
            rewritten_attrs.push(syn::parse_quote! {
                #[cfg_attr(not(feature = "long_json"), serde(rename = #short, alias = #long))]
            });
        } else {
            rewritten_attrs.push(attr);
        }
    }

    Ok(rewritten_attrs)
}

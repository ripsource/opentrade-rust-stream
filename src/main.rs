use env_logger::*;
use radix_common::math::Decimal;
use radix_common::network::NetworkDefinition;
use radix_common::prelude::Instant;
use radix_common::types::NonFungibleGlobalId;
use radix_common::{
    data::scrypto::model::NonFungibleLocalId,
    types::{ComponentAddress, ResourceAddress},
    ScryptoSbor,
};
use radix_event_stream::encodings::encode_bech32m;
use radix_event_stream::error::EventHandlerError;
use radix_event_stream::event_handler::EventHandlerContext;
use radix_event_stream::sources::gateway::GatewayTransactionStream;
use radix_event_stream::{anyhow, macros::event_handler};
use radix_event_stream::{event_handler::HandlerRegistry, processor::TransactionStreamProcessor};
use redis::Commands;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub const PACKAGE_ADDRESS: &str =
    "package_tdx_2_1p4aaxp4w7l3wmvcwtrap2t99mjx2s47npmnuac5dekdegh09sa7jva";

#[derive(ScryptoSbor, Debug)]
pub struct Listing {
    /// The permissions that a secondary seller must have to sell an NFT. This is used to ensure that only selected
    /// marketplaces or private buyers can buy an NFT.
    secondary_seller_permissions: Vec<ResourceAddress>,
    /// The seller is able to decide what currency they want to sell their NFT in (e.g. XRD, FLOOP, EARLY, HUG)
    currency: ResourceAddress,
    /// The price of the NFT - this price will be subject to marketplace fees and creator royalties which are taken as a % of this amount.
    price: Decimal,
    /// The NFTGID being recorded is potentially redundant as it is the key of the listing in the listings key value store.
    /// The actual NFT is stored in the key value store of vaults separately.
    nfgid: NonFungibleGlobalId,
    ///
    /// Because you can construct transactions atomically on Radix - you could technically list a Royalty NFT for 0 XRD,
    // then in the same transaction, purchase the NFT to another account. This would be a way to send an NFT to another user without paying a royalty
    // potentially.

    // To combat this we can store a time on a listing of the exact second a listing was made. We then block users from purchasing
    // a listing within the same second it was listed. This would prevent the above scenario from happening during normal network usage
    // where transactions are processed in a few seconds. Idealy, we could get more granular than seconds, but this seems like a pragmatic
    // solution for now.
    time_of_listing: Instant,
}

#[derive(ScryptoSbor, Debug)]
pub struct ParsedListing {
    pub secondary_seller_permissions: Vec<String>,
    pub currency: String,
    pub price: String,
    pub resource_address: String,
    pub nflid: String,
    pub time_of_listing: i64,
}

#[derive(ScryptoSbor, Debug)]
struct ListingCreated {
    listing: Listing,
    nft_id: NonFungibleGlobalId,
}

#[derive(ScryptoSbor, Debug)]
struct ListingUpdated {
    listing: Listing,
    nft_id: NonFungibleGlobalId,
}

#[derive(ScryptoSbor, Debug)]
struct ListingCanceled {
    listing: Listing,
    nft_id: NonFungibleGlobalId,
}

#[derive(ScryptoSbor, Debug)]
struct ListingPurchased {
    listing: Listing,
    nft_id: NonFungibleGlobalId,
}

#[derive(Debug)]
pub struct ParsedComponentCreated {
    pub component: String,
    pub creator_badge: String,
    pub creator_badge_local: String,
}

impl ListingCreated {
    pub fn parsed(&self, network: &NetworkDefinition) -> anyhow::Result<ParsedListing> {
        let (resource, local) = self.listing.nfgid.clone().into_parts();

        let resource_address = encode_bech32m(resource.as_node_id().as_bytes(), &network)?;
        let nflid = local.to_string();
        let price = self.listing.price.to_string();
        let currency = encode_bech32m(self.listing.currency.as_node_id().as_bytes(), &network)?;
        let time_of_listing = self.listing.time_of_listing.seconds_since_unix_epoch;
        let secondary_seller_permissions = self
            .listing
            .secondary_seller_permissions
            .iter()
            .map(|x| encode_bech32m(x.as_node_id().as_bytes(), &network))
            .collect::<Result<Vec<String>, _>>()?;

        Ok(ParsedListing {
            secondary_seller_permissions,
            currency,
            price,
            resource_address,
            nflid,
            time_of_listing,
        })
    }
}

impl ListingPurchased {
    pub fn parsed(&self, network: &NetworkDefinition) -> anyhow::Result<ParsedListing> {
        let (resource, local) = self.listing.nfgid.clone().into_parts();

        let resource_address = encode_bech32m(resource.as_node_id().as_bytes(), &network)?;
        let nflid = local.to_string();
        let price = self.listing.price.to_string();
        let currency = encode_bech32m(self.listing.currency.as_node_id().as_bytes(), &network)?;
        let time_of_listing = self.listing.time_of_listing.seconds_since_unix_epoch;
        let secondary_seller_permissions = self
            .listing
            .secondary_seller_permissions
            .iter()
            .map(|x| encode_bech32m(x.as_node_id().as_bytes(), &network))
            .collect::<Result<Vec<String>, _>>()?;

        Ok(ParsedListing {
            secondary_seller_permissions,
            currency,
            price,
            resource_address,
            nflid,
            time_of_listing,
        })
    }
}

impl ListingCanceled {
    pub fn parsed(&self, network: &NetworkDefinition) -> anyhow::Result<ParsedListing> {
        let (resource, local) = self.listing.nfgid.clone().into_parts();

        let resource_address = encode_bech32m(resource.as_node_id().as_bytes(), &network)?;
        let nflid = local.to_string();
        let price = self.listing.price.to_string();
        let currency = encode_bech32m(self.listing.currency.as_node_id().as_bytes(), &network)?;
        let time_of_listing = self.listing.time_of_listing.seconds_since_unix_epoch;
        let secondary_seller_permissions = self
            .listing
            .secondary_seller_permissions
            .iter()
            .map(|x| encode_bech32m(x.as_node_id().as_bytes(), &network))
            .collect::<Result<Vec<String>, _>>()?;

        Ok(ParsedListing {
            secondary_seller_permissions,
            currency,
            price,
            resource_address,
            nflid,
            time_of_listing,
        })
    }
}

impl ListingUpdated {
    pub fn parsed(&self, network: &NetworkDefinition) -> anyhow::Result<ParsedListing> {
        let (resource, local) = self.listing.nfgid.clone().into_parts();

        let resource_address = encode_bech32m(resource.as_node_id().as_bytes(), &network)?;
        let nflid = local.to_string();
        let price = self.listing.price.to_string();
        let currency = encode_bech32m(self.listing.currency.as_node_id().as_bytes(), &network)?;
        let time_of_listing = self.listing.time_of_listing.seconds_since_unix_epoch;
        let secondary_seller_permissions = self
            .listing
            .secondary_seller_permissions
            .iter()
            .map(|x| encode_bech32m(x.as_node_id().as_bytes(), &network))
            .collect::<Result<Vec<String>, _>>()?;

        Ok(ParsedListing {
            secondary_seller_permissions,
            currency,
            price,
            resource_address,
            nflid,
            time_of_listing,
        })
    }
}

#[event_handler]
async fn handle_listing_created_event(
    context: EventHandlerContext<State>,
    event: ListingCreated,
) -> Result<(), EventHandlerError> {
    let event = event.parsed(&context.state.network).unwrap();
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not open connection to redis: {}", e))
    })?;
    let mut con = client.get_connection().map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not get connection to redis: {}", e))
    })?;

    // Publish a message
    let _: () = con.publish("events", event.resource_address).map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not publish message to redis: {}", e))
    })?;
    Ok(())
}

#[event_handler]
async fn handle_listing_purchased_event(
    context: EventHandlerContext<State>,
    event: ListingPurchased,
) -> Result<(), EventHandlerError> {
    let event = event.parsed(&context.state.network).unwrap();
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not open connection to redis: {}", e))
    })?;
    let mut con = client.get_connection().map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not get connection to redis: {}", e))
    })?;

    // Publish a message
    let _: () = con.publish("events", event.resource_address).map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not publish message to redis: {}", e))
    })?;
    Ok(())
}

#[event_handler]
async fn handle_listing_canceled_event(
    context: EventHandlerContext<State>,
    event: ListingCanceled,
) -> Result<(), EventHandlerError> {
    let event = event.parsed(&context.state.network).unwrap();
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not open connection to redis: {}", e))
    })?;
    let mut con = client.get_connection().map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not get connection to redis: {}", e))
    })?;

    // Publish a message
    let _: () = con.publish("events", event.resource_address).map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not publish message to redis: {}", e))
    })?;
    Ok(())
}

#[event_handler]
async fn handle_listing_updated_event(
    context: EventHandlerContext<State>,
    event: ListingUpdated,
) -> Result<(), EventHandlerError> {
    let event = event.parsed(&context.state.network).unwrap();
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not open connection to redis: {}", e))
    })?;
    let mut con = client.get_connection().map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not get connection to redis: {}", e))
    })?;

    // Publish a message
    let _: () = con.publish("events", event.resource_address).map_err(|e| {
        EventHandlerError::EventRetryError(anyhow!("Could not publish message to redis: {}", e))
    })?;
    Ok(())
}

// Define a global state
#[derive(Debug)]
pub struct State {
    pub network: NetworkDefinition,
    pub gateway_client: Arc<radix_client::GatewayClientAsync>,
}

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let mut handler_registry = HandlerRegistry::new();

    handler_registry.add_handler(
        "package_tdx_2_1p4aaxp4w7l3wmvcwtrap2t99mjx2s47npmnuac5dekdegh09sa7jva",
        "ListingCreated",
        handle_listing_created_event,
    );

    handler_registry.add_handler(
        PACKAGE_ADDRESS,
        "ListingPurchased",
        handle_listing_purchased_event,
    );

    handler_registry.add_handler(
        PACKAGE_ADDRESS,
        "ListingCanceled",
        handle_listing_canceled_event,
    );

    handler_registry.add_handler(
        PACKAGE_ADDRESS,
        "ListingUpdated",
        handle_listing_updated_event,
    );

    let gateway_client = radix_client::GatewayClientAsync::new(
        radix_client::constants::PUBLIC_GATEWAY_URL.to_string(),
    );

    let state = State {
        gateway_client: Arc::new(gateway_client),
        network: NetworkDefinition::from_str("stokenet").unwrap(),
    };

    let stream = GatewayTransactionStream::new()
        .gateway_url("https://stokenet.radixdlt.com".to_string())
        .from_state_version(22774156)
        .buffer_capacity(1000)
        .limit_per_page(100);

    TransactionStreamProcessor::new(stream, handler_registry, state)
        .default_logger_with_report_interval(Duration::from_secs(1))
        .run()
        .await
        .unwrap();
}

exports = async function(changeEvent) {
    // Just notify that something new arrived
    const listing = changeEvent.fullDocument;
    
    console.log(`New listing inserted: ${listing.listing_ref}`);
    
    // Ping your service
    const serviceUrl = context.values.get("NOTIFICATION_SERVICE_URL");
    
    try {
        await context.http.post({
            url: serviceUrl,
            body: JSON.stringify({
                event: 'new_listing',
                listing_ref: listing.listing_ref,
                timestamp: new Date().toISOString()
            }),
            headers: {
                'Content-Type': ['application/json']
            }
        });
        
        console.log(`✅ Pinged notification service`);
    } catch (error) {
        console.error(`❌ Failed to ping service: ${error.message}`);
    }
};

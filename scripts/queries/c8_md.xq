<results> {
    for $op in collection('auction')/site/open_auctions/open_auction
    where count($op/bidder) > 5
    return
        <open_auctions_with_more_than_5_bidders>
            <auction>
                {$op}
            </auction>
        </open_auctions_with_more_than_5_bidders>
} </results>

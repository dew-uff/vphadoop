<results>{
for $oa in doc('auction.xml')/site/open_auctions/open_auction
for $pe in doc('auction.xml')/site/people/person
where $pe/profile/education = "College" and $oa/type = "Featured" and $pe/@id = $oa/seller/@person
order by $pe/address/country
return
    <seller>
        <country>{ $pe/address/country }</country>
        <person>{ $pe/name }</person>
        <item id='{$oa/itemref/@item}'>
            <initial_bid>{$oa/initial}</initial_bid>
            <current_bid>{$oa/current}</current_bid>
        </item>
    </seller>
}</results>

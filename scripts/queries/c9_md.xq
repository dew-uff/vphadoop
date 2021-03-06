<results>{
for $pe in collection('auction')//person
for $oa in collection('auction')/site/open_auctions/open_auction
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

<results> {
    for $it in doc('auction.xml')/site/regions/samerica/item
    for $pe in doc('auction.xml')/site/people/person
    where $it/incategory/@category = "category251" and $pe/profile/interest/@category = $it/incategory/@category and $pe/profile/education = "Graduate School"
    return
        <match_cat_251>
            {$pe}
        </match_cat_251>
} </results>

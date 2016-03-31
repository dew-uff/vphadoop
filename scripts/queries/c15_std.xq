<results> {
    for $it in doc('expdb/auction.xml')/site/regions/samerica/item
    for $pe in doc('expdb/auction.xml')/site/people/person
    where $it/incategory/@category = "category250" and $pe/profile/interest/@category = $it/incategory/@category and $pe/profile/education = "Graduate School"
    return
        <match_cat_250>
            {$pe}
        </match_cat_250>
} </results>

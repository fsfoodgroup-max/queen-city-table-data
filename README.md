# Queen City Table data

The restaurant list behind [queencitytable.com](https://queencitytable.com). A GitHub Action rebuilds it every night.

## What the nightly job does

1. Checks for a new monthly release of [Overture Maps](https://overturemaps.org) place data and, when there is one, pulls every food and drink place in the Charlotte area.
2. Keeps open, full-service local restaurants. Bakeries, dessert shops, coffee shops, food trucks and fast food are left out.
3. Drops out-of-town chains: any name with 8 or more locations outside North and South Carolina, plus everything on the exclude list.
4. Visits about 400 restaurant websites a night (respecting robots.txt) to see whether they offer delivery, online ordering, or Resy, OpenTable or Tock reservations. Every site gets rechecked about once a week. A site that fails three checks in a row is treated as closed.
5. Merges the hand-curated staff picks and writes `data/restaurants.json`, which the website reads.

## Files you may want to edit

- `config/lists.json`: names to always keep (local groups with many locations) or always exclude, plus the filter settings.
- `data/staff_picks.json`: the curated staff picks, with full descriptions, prices and what to order.
- `config/neighborhoods.json`: neighborhood centers used to place each restaurant.

Edits take effect on the next nightly run. To run it right away, open the **Actions** tab, choose **Nightly restaurant update**, and click **Run workflow**.

## Data sources and licenses

Place data comes from the Overture Maps Foundation, released under CDLA Permissive 2.0 and other open licenses listed in each record's sources. Delivery, ordering and reservation details come from each restaurant's own website. Staff picks are written by Queen City Table.

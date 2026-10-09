-- Demo stock, loaded only with the `local` profile. Inserts missing SKUs and never overwrites existing levels,
-- so re-running it can't break reservations that are already counted in `reserved`.
INSERT INTO product_stock (sku, available, reserved) VALUES
    ('MUG-RED',    100, 0),
    ('MUG-BLUE',    50, 0),
    ('TEA-GREEN',  200, 0),
    ('TEA-BLACK',    0, 0),   -- always out of stock: demo for InventoryRejected
    ('LAMP-DESK',    3, 0)    -- low stock: demo for races on the last units
ON CONFLICT (sku) DO NOTHING;

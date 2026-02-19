<?php
defined('ABSPATH') || exit;

class TCCHE_OB_Checkout {

    public static function init() {
        add_action('woocommerce_checkout_after_order_review', [__CLASS__, 'render_bumps_after_order_review'], 5);
        add_action('woocommerce_review_order_before_payment', [__CLASS__, 'render_bumps_before_payment'], 5);
        add_action('woocommerce_after_checkout_billing_form', [__CLASS__, 'render_bumps_after_customer_details'], 5);
        add_action('woocommerce_review_order_after_submit', [__CLASS__, 'render_bumps_after_place_order'], 5);

        add_action('woocommerce_checkout_create_order_line_item', [__CLASS__, 'tag_bump_line_item'], 10, 4);
        add_action('woocommerce_checkout_order_processed', [__CLASS__, 'process_accepted_bumps'], 10, 3);
    }

    public static function enqueue_assets() {
        if (!is_checkout()) {
            return;
        }

        wp_enqueue_style(
            'tcche-ob-frontend',
            TCCHE_OB_PLUGIN_URL . 'assets/css/frontend.css',
            [],
            TCCHE_OB_VERSION
        );

        wp_enqueue_script(
            'tcche-ob-frontend',
            TCCHE_OB_PLUGIN_URL . 'assets/js/frontend.js',
            ['jquery'],
            TCCHE_OB_VERSION,
            true
        );

        wp_localize_script('tcche-ob-frontend', 'tccheOBFront', [
            'ajax_url' => admin_url('admin-ajax.php'),
            'nonce'    => wp_create_nonce('tcche_ob_frontend'),
        ]);
    }

    public static function render_bumps_after_order_review() {
        self::render_bumps('after_order_review');
    }

    public static function render_bumps_before_payment() {
        self::render_bumps('before_payment');
    }

    public static function render_bumps_after_customer_details() {
        self::render_bumps('after_customer_details');
    }

    public static function render_bumps_after_place_order() {
        self::render_bumps('after_place_order');
    }

    private static function render_bumps($position) {
        $active_bumps = TCCHE_OB_Post_Type::get_active_bumps();
        $cart_product_ids = self::get_cart_product_ids();
        $cart_category_ids = self::get_cart_category_ids();

        foreach ($active_bumps as $bump) {
            if ($bump['position'] !== $position) {
                continue;
            }

            if (!self::should_show_bump($bump, $cart_product_ids, $cart_category_ids)) {
                continue;
            }

            // Don't show if bump product is already in cart
            if (in_array($bump['bump_product_id'], $cart_product_ids, true)) {
                continue;
            }

            $product = wc_get_product($bump['bump_product_id']);
            if (!$product || !$product->is_purchasable()) {
                continue;
            }

            TCCHE_OB_Analytics::track_impression($bump['id']);

            self::render_bump_template($bump, $product);
        }
    }

    private static function should_show_bump($bump, $cart_product_ids, $cart_category_ids) {
        $trigger_products = $bump['trigger_product_ids'];
        $trigger_categories = $bump['trigger_category_ids'];

        $has_product_trigger = !empty($trigger_products);
        $has_category_trigger = !empty($trigger_categories);

        // No triggers = show to everyone
        if (!$has_product_trigger && !$has_category_trigger) {
            return true;
        }

        if ($has_product_trigger && array_intersect($trigger_products, $cart_product_ids)) {
            return true;
        }

        if ($has_category_trigger && array_intersect($trigger_categories, $cart_category_ids)) {
            return true;
        }

        return false;
    }

    private static function render_bump_template($bump, $product) {
        $bump_price     = $bump['bump_price'];
        $original_price = $bump['original_price'];
        $has_discount   = $bump['discount_type'] !== 'none' && $bump_price < $original_price;
        $currency       = get_woocommerce_currency_symbol();
        $design_style   = $bump['design_style'] ?? 'classic';

        include TCCHE_OB_PLUGIN_DIR . 'templates/bump-offer.php';
    }

    private static function get_cart_product_ids() {
        $ids = [];
        if (WC()->cart) {
            foreach (WC()->cart->get_cart() as $item) {
                $ids[] = $item['product_id'];
            }
        }
        return $ids;
    }

    private static function get_cart_category_ids() {
        $cat_ids = [];
        if (WC()->cart) {
            foreach (WC()->cart->get_cart() as $item) {
                $terms = get_the_terms($item['product_id'], 'product_cat');
                if ($terms) {
                    foreach ($terms as $term) {
                        $cat_ids[] = $term->term_id;
                    }
                }
            }
        }
        return array_unique($cat_ids);
    }

    public static function tag_bump_line_item($item, $cart_item_key, $values, $order) {
        if (!empty($values['tcche_order_bump_id'])) {
            $item->add_meta_data('_tcche_order_bump_id', absint($values['tcche_order_bump_id']), true);
        }
    }

    public static function process_accepted_bumps($order_id, $posted_data, $order) {
        foreach ($order->get_items() as $item) {
            $bump_id = $item->get_meta('_tcche_order_bump_id');
            if ($bump_id) {
                TCCHE_OB_Analytics::track_conversion($bump_id, $order_id, $item->get_total());
            }
        }
    }
}

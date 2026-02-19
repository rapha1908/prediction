<?php
defined('ABSPATH') || exit;

class TCCHE_OB_Ajax {

    public static function init() {
        // Frontend: accept/remove bump
        add_action('wp_ajax_tcche_ob_accept_bump', [__CLASS__, 'accept_bump']);
        add_action('wp_ajax_nopriv_tcche_ob_accept_bump', [__CLASS__, 'accept_bump']);
        add_action('wp_ajax_tcche_ob_remove_bump', [__CLASS__, 'remove_bump']);
        add_action('wp_ajax_nopriv_tcche_ob_remove_bump', [__CLASS__, 'remove_bump']);

        // Admin: save bump, delete bump, search products, get analytics
        add_action('wp_ajax_tcche_ob_save_bump', [__CLASS__, 'save_bump']);
        add_action('wp_ajax_tcche_ob_delete_bump', [__CLASS__, 'delete_bump']);
        add_action('wp_ajax_tcche_ob_search_products', [__CLASS__, 'search_products']);
        add_action('wp_ajax_tcche_ob_get_analytics', [__CLASS__, 'get_analytics']);
    }

    // --- Frontend ---

    public static function accept_bump() {
        check_ajax_referer('tcche_ob_frontend', 'nonce');

        $bump_id    = absint($_POST['bump_id'] ?? 0);
        $bump       = TCCHE_OB_Post_Type::get_bump($bump_id);

        if (!$bump) {
            wp_send_json_error(['message' => __('Invalid order bump.', 'tcche-order-bump')]);
        }

        $product = wc_get_product($bump['bump_product_id']);
        if (!$product) {
            wp_send_json_error(['message' => __('Product not found.', 'tcche-order-bump')]);
        }

        // Check if already in cart
        foreach (WC()->cart->get_cart() as $cart_key => $cart_item) {
            if ($cart_item['product_id'] == $bump['bump_product_id'] && !empty($cart_item['tcche_order_bump_id'])) {
                wp_send_json_success(['message' => __('Already added.', 'tcche-order-bump'), 'already_in_cart' => true]);
            }
        }

        $cart_item_data = [
            'tcche_order_bump_id' => $bump_id,
        ];

        // Apply discount via custom price
        if ($bump['discount_type'] !== 'none') {
            $cart_item_data['tcche_ob_custom_price'] = $bump['bump_price'];
        }

        $cart_key = WC()->cart->add_to_cart($bump['bump_product_id'], 1, 0, [], $cart_item_data);

        if ($cart_key) {
            TCCHE_OB_Analytics::track_conversion($bump_id, 0, $bump['bump_price']);

            wp_send_json_success([
                'message'  => __('Added to your order!', 'tcche-order-bump'),
                'cart_key' => $cart_key,
                'fragments' => self::get_refreshed_fragments(),
            ]);
        }

        wp_send_json_error(['message' => __('Could not add product.', 'tcche-order-bump')]);
    }

    public static function remove_bump() {
        check_ajax_referer('tcche_ob_frontend', 'nonce');

        $bump_id = absint($_POST['bump_id'] ?? 0);

        foreach (WC()->cart->get_cart() as $cart_key => $cart_item) {
            if (!empty($cart_item['tcche_order_bump_id']) && $cart_item['tcche_order_bump_id'] == $bump_id) {
                WC()->cart->remove_cart_item($cart_key);
                wp_send_json_success([
                    'message'   => __('Removed from your order.', 'tcche-order-bump'),
                    'fragments' => self::get_refreshed_fragments(),
                ]);
            }
        }

        wp_send_json_error(['message' => __('Item not found in cart.', 'tcche-order-bump')]);
    }

    private static function get_refreshed_fragments() {
        WC()->cart->calculate_totals();

        ob_start();
        woocommerce_order_review();
        $order_review = ob_get_clean();

        return [
            '.woocommerce-checkout-review-order-table' => $order_review,
        ];
    }

    // --- Admin ---

    public static function save_bump() {
        check_ajax_referer('tcche_ob_admin', 'nonce');

        if (!current_user_can('manage_woocommerce')) {
            wp_send_json_error(['message' => __('Permission denied.', 'tcche-order-bump')]);
        }

        $bump_id = absint($_POST['bump_id'] ?? 0);
        $data = [
            'title'               => sanitize_text_field($_POST['title'] ?? ''),
            'bump_product_id'     => absint($_POST['bump_product_id'] ?? 0),
            'trigger_product_ids' => array_map('absint', (array) ($_POST['trigger_product_ids'] ?? [])),
            'trigger_category_ids'=> array_map('absint', (array) ($_POST['trigger_category_ids'] ?? [])),
            'discount_type'       => sanitize_text_field($_POST['discount_type'] ?? 'none'),
            'discount_value'      => floatval($_POST['discount_value'] ?? 0),
            'headline'            => sanitize_text_field($_POST['headline'] ?? ''),
            'description'         => wp_kses_post($_POST['description'] ?? ''),
            'position'            => sanitize_text_field($_POST['position'] ?? 'after_order_review'),
            'design_style'        => sanitize_text_field($_POST['design_style'] ?? 'classic'),
            'priority'            => absint($_POST['priority'] ?? 10),
            'status'              => sanitize_text_field($_POST['status'] ?? 'publish'),
        ];

        if ($bump_id) {
            $result = TCCHE_OB_Post_Type::update_bump($bump_id, $data);
        } else {
            $result = TCCHE_OB_Post_Type::create_bump($data);
        }

        if (is_wp_error($result)) {
            wp_send_json_error(['message' => $result->get_error_message()]);
        }

        wp_send_json_success([
            'bump_id' => is_int($result) ? $result : $bump_id,
            'message' => __('Order bump saved.', 'tcche-order-bump'),
        ]);
    }

    public static function delete_bump() {
        check_ajax_referer('tcche_ob_admin', 'nonce');

        if (!current_user_can('manage_woocommerce')) {
            wp_send_json_error(['message' => __('Permission denied.', 'tcche-order-bump')]);
        }

        $bump_id = absint($_POST['bump_id'] ?? 0);
        $result = TCCHE_OB_Post_Type::delete_bump($bump_id);

        if (is_wp_error($result)) {
            wp_send_json_error(['message' => $result->get_error_message()]);
        }

        wp_send_json_success(['message' => __('Order bump deleted.', 'tcche-order-bump')]);
    }

    public static function search_products() {
        check_ajax_referer('tcche_ob_admin', 'nonce');

        if (!current_user_can('manage_woocommerce')) {
            wp_send_json_error([]);
        }

        $term = sanitize_text_field($_GET['term'] ?? '');
        if (strlen($term) < 2) {
            wp_send_json_success([]);
        }

        $products = wc_get_products([
            'status' => 'publish',
            'limit'  => 20,
            's'      => $term,
        ]);

        $results = [];
        foreach ($products as $product) {
            $results[] = [
                'id'    => $product->get_id(),
                'text'  => sprintf('%s (#%d) - %s', $product->get_name(), $product->get_id(), wc_price($product->get_price())),
                'name'  => $product->get_name(),
                'price' => $product->get_price(),
            ];
        }

        wp_send_json_success($results);
    }

    public static function get_analytics() {
        check_ajax_referer('tcche_ob_admin', 'nonce');

        if (!current_user_can('manage_woocommerce')) {
            wp_send_json_error([]);
        }

        $bump_id   = absint($_GET['bump_id'] ?? 0);
        $date_from = sanitize_text_field($_GET['date_from'] ?? gmdate('Y-m-d', strtotime('-30 days')));
        $date_to   = sanitize_text_field($_GET['date_to'] ?? gmdate('Y-m-d'));

        $summary = TCCHE_OB_Analytics::get_stats([
            'bump_id'   => $bump_id,
            'date_from' => $date_from,
            'date_to'   => $date_to,
        ]);

        $daily = TCCHE_OB_Analytics::get_daily_stats([
            'bump_id'   => $bump_id,
            'date_from' => $date_from,
            'date_to'   => $date_to,
        ]);

        $by_bump = TCCHE_OB_Analytics::get_stats_by_bump([
            'date_from' => $date_from,
            'date_to'   => $date_to,
        ]);

        wp_send_json_success([
            'summary' => $summary,
            'daily'   => $daily,
            'by_bump' => $by_bump,
        ]);
    }
}

// Apply custom prices for bump products in cart
add_action('woocommerce_before_calculate_totals', function ($cart) {
    if (is_admin() && !defined('DOING_AJAX')) {
        return;
    }

    foreach ($cart->get_cart() as $cart_item) {
        if (isset($cart_item['tcche_ob_custom_price'])) {
            $cart_item['data']->set_price($cart_item['tcche_ob_custom_price']);
        }
    }
}, 99);

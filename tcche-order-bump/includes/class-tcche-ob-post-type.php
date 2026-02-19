<?php
defined('ABSPATH') || exit;

class TCCHE_OB_Post_Type {

    const POST_TYPE = 'tcche_order_bump';

    public static function register() {
        register_post_type(self::POST_TYPE, [
            'labels' => [
                'name'               => __('Order Bumps', 'tcche-order-bump'),
                'singular_name'      => __('Order Bump', 'tcche-order-bump'),
                'add_new'            => __('Add New Bump', 'tcche-order-bump'),
                'add_new_item'       => __('Add New Order Bump', 'tcche-order-bump'),
                'edit_item'          => __('Edit Order Bump', 'tcche-order-bump'),
                'new_item'           => __('New Order Bump', 'tcche-order-bump'),
                'view_item'          => __('View Order Bump', 'tcche-order-bump'),
                'search_items'       => __('Search Order Bumps', 'tcche-order-bump'),
                'not_found'          => __('No order bumps found', 'tcche-order-bump'),
                'not_found_in_trash' => __('No order bumps found in Trash', 'tcche-order-bump'),
            ],
            'public'       => false,
            'show_ui'      => false, // We use our own admin UI
            'has_archive'  => false,
            'supports'     => ['title'],
            'map_meta_cap' => true,
            'capability_type' => 'post',
        ]);
    }

    public static function create_bump($data) {
        $defaults = [
            'title'              => '',
            'bump_product_id'    => 0,
            'trigger_product_ids'=> [],
            'trigger_category_ids' => [],
            'discount_type'      => 'none', // none, percentage, fixed
            'discount_value'     => 0,
            'headline'           => __('Special Offer!', 'tcche-order-bump'),
            'description'        => '',
            'position'           => 'after_order_review',
            'design_style'       => 'classic',
            'priority'           => 10,
            'status'             => 'publish',
        ];

        $data = wp_parse_args($data, $defaults);

        $post_id = wp_insert_post([
            'post_type'   => self::POST_TYPE,
            'post_title'  => sanitize_text_field($data['title']),
            'post_status' => $data['status'] === 'publish' ? 'publish' : 'draft',
        ]);

        if (is_wp_error($post_id)) {
            return $post_id;
        }

        self::update_bump_meta($post_id, $data);

        return $post_id;
    }

    public static function update_bump($bump_id, $data) {
        $post = get_post($bump_id);
        if (!$post || $post->post_type !== self::POST_TYPE) {
            return new WP_Error('invalid_bump', __('Invalid order bump ID', 'tcche-order-bump'));
        }

        $update_args = ['ID' => $bump_id];

        if (isset($data['title'])) {
            $update_args['post_title'] = sanitize_text_field($data['title']);
        }
        if (isset($data['status'])) {
            $update_args['post_status'] = $data['status'] === 'publish' ? 'publish' : 'draft';
        }

        wp_update_post($update_args);
        self::update_bump_meta($bump_id, $data);

        return $bump_id;
    }

    private static function update_bump_meta($post_id, $data) {
        $meta_fields = [
            'bump_product_id',
            'trigger_product_ids',
            'trigger_category_ids',
            'discount_type',
            'discount_value',
            'headline',
            'description',
            'position',
            'design_style',
            'priority',
        ];

        foreach ($meta_fields as $field) {
            if (!isset($data[$field])) {
                continue;
            }

            $value = $data[$field];

            if (in_array($field, ['trigger_product_ids', 'trigger_category_ids'], true)) {
                $value = array_map('absint', (array) $value);
            } elseif (in_array($field, ['bump_product_id', 'priority'], true)) {
                $value = absint($value);
            } elseif ($field === 'discount_value') {
                $value = floatval($value);
            } else {
                $value = sanitize_text_field($value);
            }

            update_post_meta($post_id, '_tcche_ob_' . $field, $value);
        }
    }

    public static function get_bump($bump_id) {
        $post = get_post($bump_id);
        if (!$post || $post->post_type !== self::POST_TYPE) {
            return null;
        }
        return self::format_bump($post);
    }

    public static function get_bumps($args = []) {
        $defaults = [
            'post_type'      => self::POST_TYPE,
            'posts_per_page' => -1,
            'post_status'    => 'any',
            'orderby'        => 'date',
            'order'          => 'DESC',
        ];

        $query = new WP_Query(wp_parse_args($args, $defaults));
        $bumps = [];

        foreach ($query->posts as $post) {
            $bumps[] = self::format_bump($post);
        }

        return $bumps;
    }

    public static function get_active_bumps() {
        return self::get_bumps(['post_status' => 'publish']);
    }

    public static function delete_bump($bump_id) {
        $post = get_post($bump_id);
        if (!$post || $post->post_type !== self::POST_TYPE) {
            return new WP_Error('invalid_bump', __('Invalid order bump ID', 'tcche-order-bump'));
        }
        return wp_delete_post($bump_id, true);
    }

    public static function format_bump($post) {
        $product_id = get_post_meta($post->ID, '_tcche_ob_bump_product_id', true);
        $product    = wc_get_product($product_id);

        $discount_type  = get_post_meta($post->ID, '_tcche_ob_discount_type', true) ?: 'none';
        $discount_value = (float) get_post_meta($post->ID, '_tcche_ob_discount_value', true);

        $bump_price = 0;
        $original_price = 0;
        if ($product) {
            $original_price = (float) $product->get_price();
            $bump_price = self::calculate_bump_price($original_price, $discount_type, $discount_value);
        }

        return [
            'id'                   => $post->ID,
            'title'                => $post->post_title,
            'status'               => $post->post_status,
            'bump_product_id'      => absint($product_id),
            'bump_product_name'    => $product ? $product->get_name() : '',
            'bump_product_image'   => $product ? wp_get_attachment_image_url($product->get_image_id(), 'thumbnail') : '',
            'original_price'       => $original_price,
            'bump_price'           => $bump_price,
            'trigger_product_ids'  => get_post_meta($post->ID, '_tcche_ob_trigger_product_ids', true) ?: [],
            'trigger_category_ids' => get_post_meta($post->ID, '_tcche_ob_trigger_category_ids', true) ?: [],
            'discount_type'        => $discount_type,
            'discount_value'       => $discount_value,
            'headline'             => get_post_meta($post->ID, '_tcche_ob_headline', true) ?: '',
            'description'          => get_post_meta($post->ID, '_tcche_ob_description', true) ?: '',
            'position'             => get_post_meta($post->ID, '_tcche_ob_position', true) ?: 'after_order_review',
            'design_style'         => get_post_meta($post->ID, '_tcche_ob_design_style', true) ?: 'classic',
            'priority'             => absint(get_post_meta($post->ID, '_tcche_ob_priority', true)) ?: 10,
            'created_at'           => $post->post_date,
            'updated_at'           => $post->post_modified,
        ];
    }

    public static function calculate_bump_price($original_price, $discount_type, $discount_value) {
        switch ($discount_type) {
            case 'percentage':
                return round($original_price * (1 - $discount_value / 100), 2);
            case 'fixed':
                return max(0, round($original_price - $discount_value, 2));
            default:
                return $original_price;
        }
    }
}

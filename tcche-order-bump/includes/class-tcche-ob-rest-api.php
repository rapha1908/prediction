<?php
defined('ABSPATH') || exit;

class TCCHE_OB_REST_API {

    const NAMESPACE = 'tcche-ob/v1';

    public static function register_routes() {

        // --- Order Bumps CRUD ---

        register_rest_route(self::NAMESPACE, '/bumps', [
            [
                'methods'             => WP_REST_Server::READABLE,
                'callback'            => [__CLASS__, 'get_bumps'],
                'permission_callback' => [__CLASS__, 'check_admin_permission'],
            ],
            [
                'methods'             => WP_REST_Server::CREATABLE,
                'callback'            => [__CLASS__, 'create_bump'],
                'permission_callback' => [__CLASS__, 'check_admin_permission'],
                'args'                => self::get_bump_args(true),
            ],
        ]);

        register_rest_route(self::NAMESPACE, '/bumps/(?P<id>\d+)', [
            [
                'methods'             => WP_REST_Server::READABLE,
                'callback'            => [__CLASS__, 'get_bump'],
                'permission_callback' => [__CLASS__, 'check_admin_permission'],
            ],
            [
                'methods'             => 'PUT, PATCH',
                'callback'            => [__CLASS__, 'update_bump'],
                'permission_callback' => [__CLASS__, 'check_admin_permission'],
                'args'                => self::get_bump_args(false),
            ],
            [
                'methods'             => WP_REST_Server::DELETABLE,
                'callback'            => [__CLASS__, 'delete_bump'],
                'permission_callback' => [__CLASS__, 'check_admin_permission'],
            ],
        ]);

        // --- Analytics ---

        register_rest_route(self::NAMESPACE, '/analytics/summary', [
            'methods'             => WP_REST_Server::READABLE,
            'callback'            => [__CLASS__, 'get_analytics_summary'],
            'permission_callback' => [__CLASS__, 'check_admin_permission'],
            'args' => [
                'bump_id'   => ['type' => 'integer', 'default' => 0],
                'date_from' => ['type' => 'string', 'default' => gmdate('Y-m-d', strtotime('-30 days'))],
                'date_to'   => ['type' => 'string', 'default' => gmdate('Y-m-d')],
            ],
        ]);

        register_rest_route(self::NAMESPACE, '/analytics/by-bump', [
            'methods'             => WP_REST_Server::READABLE,
            'callback'            => [__CLASS__, 'get_analytics_by_bump'],
            'permission_callback' => [__CLASS__, 'check_admin_permission'],
            'args' => [
                'date_from' => ['type' => 'string', 'default' => gmdate('Y-m-d', strtotime('-30 days'))],
                'date_to'   => ['type' => 'string', 'default' => gmdate('Y-m-d')],
            ],
        ]);

        register_rest_route(self::NAMESPACE, '/analytics/daily', [
            'methods'             => WP_REST_Server::READABLE,
            'callback'            => [__CLASS__, 'get_analytics_daily'],
            'permission_callback' => [__CLASS__, 'check_admin_permission'],
            'args' => [
                'bump_id'   => ['type' => 'integer', 'default' => 0],
                'date_from' => ['type' => 'string', 'default' => gmdate('Y-m-d', strtotime('-30 days'))],
                'date_to'   => ['type' => 'string', 'default' => gmdate('Y-m-d')],
            ],
        ]);
    }

    // --- Callbacks ---

    public static function get_bumps($request) {
        $status = $request->get_param('status') ?: 'any';
        $bumps = TCCHE_OB_Post_Type::get_bumps(['post_status' => $status]);
        return rest_ensure_response($bumps);
    }

    public static function get_bump($request) {
        $bump = TCCHE_OB_Post_Type::get_bump($request['id']);
        if (!$bump) {
            return new WP_Error('not_found', __('Order bump not found', 'tcche-order-bump'), ['status' => 404]);
        }
        return rest_ensure_response($bump);
    }

    public static function create_bump($request) {
        $data = self::extract_bump_data($request);
        $result = TCCHE_OB_Post_Type::create_bump($data);

        if (is_wp_error($result)) {
            return $result;
        }

        $bump = TCCHE_OB_Post_Type::get_bump($result);
        return rest_ensure_response($bump);
    }

    public static function update_bump($request) {
        $bump = TCCHE_OB_Post_Type::get_bump($request['id']);
        if (!$bump) {
            return new WP_Error('not_found', __('Order bump not found', 'tcche-order-bump'), ['status' => 404]);
        }

        $data = self::extract_bump_data($request);
        $result = TCCHE_OB_Post_Type::update_bump($request['id'], $data);

        if (is_wp_error($result)) {
            return $result;
        }

        $updated = TCCHE_OB_Post_Type::get_bump($request['id']);
        return rest_ensure_response($updated);
    }

    public static function delete_bump($request) {
        $bump = TCCHE_OB_Post_Type::get_bump($request['id']);
        if (!$bump) {
            return new WP_Error('not_found', __('Order bump not found', 'tcche-order-bump'), ['status' => 404]);
        }

        $result = TCCHE_OB_Post_Type::delete_bump($request['id']);
        if (is_wp_error($result)) {
            return $result;
        }

        return rest_ensure_response(['deleted' => true, 'id' => $request['id']]);
    }

    public static function get_analytics_summary($request) {
        $stats = TCCHE_OB_Analytics::get_stats([
            'bump_id'   => $request->get_param('bump_id'),
            'date_from' => $request->get_param('date_from'),
            'date_to'   => $request->get_param('date_to'),
        ]);
        return rest_ensure_response($stats);
    }

    public static function get_analytics_by_bump($request) {
        $stats = TCCHE_OB_Analytics::get_stats_by_bump([
            'date_from' => $request->get_param('date_from'),
            'date_to'   => $request->get_param('date_to'),
        ]);
        return rest_ensure_response($stats);
    }

    public static function get_analytics_daily($request) {
        $stats = TCCHE_OB_Analytics::get_daily_stats([
            'bump_id'   => $request->get_param('bump_id'),
            'date_from' => $request->get_param('date_from'),
            'date_to'   => $request->get_param('date_to'),
        ]);
        return rest_ensure_response($stats);
    }

    // --- Helpers ---

    public static function check_admin_permission() {
        return current_user_can('manage_woocommerce');
    }

    private static function get_bump_args($required = false) {
        return [
            'title' => [
                'type'     => 'string',
                'required' => $required,
                'sanitize_callback' => 'sanitize_text_field',
            ],
            'bump_product_id' => [
                'type'     => 'integer',
                'required' => $required,
            ],
            'trigger_product_ids' => [
                'type'    => 'array',
                'default' => [],
                'items'   => ['type' => 'integer'],
            ],
            'trigger_category_ids' => [
                'type'    => 'array',
                'default' => [],
                'items'   => ['type' => 'integer'],
            ],
            'discount_type' => [
                'type'    => 'string',
                'default' => 'none',
                'enum'    => ['none', 'percentage', 'fixed'],
            ],
            'discount_value' => [
                'type'    => 'number',
                'default' => 0,
            ],
            'headline' => [
                'type'    => 'string',
                'default' => 'Special Offer!',
                'sanitize_callback' => 'sanitize_text_field',
            ],
            'description' => [
                'type'    => 'string',
                'default' => '',
                'sanitize_callback' => 'wp_kses_post',
            ],
            'position' => [
                'type'    => 'string',
                'default' => 'after_order_review',
                'enum'    => ['after_order_review', 'before_payment', 'after_customer_details', 'after_place_order'],
            ],
            'design_style' => [
                'type'    => 'string',
                'default' => 'classic',
                'enum'    => ['classic', 'minimal', 'bold', 'rounded'],
            ],
            'priority' => [
                'type'    => 'integer',
                'default' => 10,
            ],
            'status' => [
                'type'    => 'string',
                'default' => 'publish',
                'enum'    => ['publish', 'draft'],
            ],
        ];
    }

    private static function extract_bump_data($request) {
        $fields = [
            'title', 'bump_product_id', 'trigger_product_ids', 'trigger_category_ids',
            'discount_type', 'discount_value', 'headline', 'description',
            'position', 'design_style', 'priority', 'status',
        ];

        $data = [];
        foreach ($fields as $field) {
            $value = $request->get_param($field);
            if ($value !== null) {
                $data[$field] = $value;
            }
        }

        return $data;
    }
}

<?php
defined('ABSPATH') || exit;

class TCCHE_OB_Admin {

    public static function register_menus() {
        add_menu_page(
            __('Order Bumps', 'tcche-order-bump'),
            __('Order Bumps', 'tcche-order-bump'),
            'manage_woocommerce',
            'tcche-order-bumps',
            [__CLASS__, 'render_list_page'],
            'dashicons-arrow-up-alt',
            56
        );

        add_submenu_page(
            'tcche-order-bumps',
            __('All Bumps', 'tcche-order-bump'),
            __('All Bumps', 'tcche-order-bump'),
            'manage_woocommerce',
            'tcche-order-bumps',
            [__CLASS__, 'render_list_page']
        );

        add_submenu_page(
            'tcche-order-bumps',
            __('Add New', 'tcche-order-bump'),
            __('Add New', 'tcche-order-bump'),
            'manage_woocommerce',
            'tcche-order-bump-new',
            [__CLASS__, 'render_edit_page']
        );

        add_submenu_page(
            'tcche-order-bumps',
            __('Analytics', 'tcche-order-bump'),
            __('Analytics', 'tcche-order-bump'),
            'manage_woocommerce',
            'tcche-order-bump-analytics',
            [__CLASS__, 'render_analytics_page']
        );

        // Hidden edit page
        add_submenu_page(
            null,
            __('Edit Order Bump', 'tcche-order-bump'),
            __('Edit', 'tcche-order-bump'),
            'manage_woocommerce',
            'tcche-order-bump-edit',
            [__CLASS__, 'render_edit_page']
        );
    }

    public static function enqueue_assets($hook) {
        $pages = [
            'toplevel_page_tcche-order-bumps',
            'order-bumps_page_tcche-order-bump-new',
            'order-bumps_page_tcche-order-bump-analytics',
            'admin_page_tcche-order-bump-edit',
        ];

        if (!in_array($hook, $pages, true)) {
            return;
        }

        wp_enqueue_style(
            'tcche-ob-admin',
            TCCHE_OB_PLUGIN_URL . 'assets/css/admin.css',
            [],
            TCCHE_OB_VERSION
        );

        wp_enqueue_script(
            'tcche-ob-admin',
            TCCHE_OB_PLUGIN_URL . 'assets/js/admin.js',
            ['jquery', 'wp-util'],
            TCCHE_OB_VERSION,
            true
        );

        if (strpos($hook, 'analytics') !== false) {
            wp_enqueue_script('chart-js', 'https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js', [], '4.4.7', true);
        }

        wp_enqueue_style('woocommerce_admin_styles');

        wp_localize_script('tcche-ob-admin', 'tccheOB', [
            'ajax_url' => admin_url('admin-ajax.php'),
            'nonce'    => wp_create_nonce('tcche_ob_admin'),
            'i18n'     => [
                'confirm_delete' => __('Are you sure you want to delete this order bump?', 'tcche-order-bump'),
                'saving'         => __('Saving...', 'tcche-order-bump'),
                'saved'          => __('Saved!', 'tcche-order-bump'),
                'error'          => __('An error occurred.', 'tcche-order-bump'),
            ],
        ]);
    }

    public static function render_list_page() {
        $bumps = TCCHE_OB_Post_Type::get_bumps();
        include TCCHE_OB_PLUGIN_DIR . 'templates/admin/bump-list.php';
    }

    public static function render_edit_page() {
        $bump_id = isset($_GET['bump_id']) ? absint($_GET['bump_id']) : 0;
        $bump = $bump_id ? TCCHE_OB_Post_Type::get_bump($bump_id) : null;

        $categories = get_terms([
            'taxonomy'   => 'product_cat',
            'hide_empty' => false,
        ]);

        include TCCHE_OB_PLUGIN_DIR . 'templates/admin/bump-edit.php';
    }

    public static function render_analytics_page() {
        include TCCHE_OB_PLUGIN_DIR . 'templates/admin/analytics-page.php';
    }
}

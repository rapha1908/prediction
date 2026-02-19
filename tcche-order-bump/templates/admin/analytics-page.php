<?php defined('ABSPATH') || exit; ?>
<div class="wrap tcche-ob-admin tcche-ob-analytics">
    <h1><?php esc_html_e('Order Bump Analytics', 'tcche-order-bump'); ?></h1>

    <!-- Date Filter -->
    <div class="tcche-ob-analytics__filters">
        <div class="tcche-ob-analytics__date-range">
            <label for="tcche-ob-date-from"><?php esc_html_e('From:', 'tcche-order-bump'); ?></label>
            <input type="date" id="tcche-ob-date-from"
                   value="<?php echo esc_attr(gmdate('Y-m-d', strtotime('-30 days'))); ?>">

            <label for="tcche-ob-date-to"><?php esc_html_e('To:', 'tcche-order-bump'); ?></label>
            <input type="date" id="tcche-ob-date-to"
                   value="<?php echo esc_attr(gmdate('Y-m-d')); ?>">

            <div class="tcche-ob-analytics__quick-dates">
                <button type="button" class="button tcche-ob-quick-date" data-days="7"><?php esc_html_e('7 days', 'tcche-order-bump'); ?></button>
                <button type="button" class="button tcche-ob-quick-date active" data-days="30"><?php esc_html_e('30 days', 'tcche-order-bump'); ?></button>
                <button type="button" class="button tcche-ob-quick-date" data-days="90"><?php esc_html_e('90 days', 'tcche-order-bump'); ?></button>
            </div>

            <button type="button" class="button button-primary" id="tcche-ob-apply-filter">
                <?php esc_html_e('Apply', 'tcche-order-bump'); ?>
            </button>
        </div>
    </div>

    <!-- Summary Cards -->
    <div class="tcche-ob-analytics__summary">
        <div class="tcche-ob-stat-card">
            <div class="tcche-ob-stat-card__icon"><span class="dashicons dashicons-visibility"></span></div>
            <div class="tcche-ob-stat-card__content">
                <span class="tcche-ob-stat-card__value" id="stat-impressions">--</span>
                <span class="tcche-ob-stat-card__label"><?php esc_html_e('Impressions', 'tcche-order-bump'); ?></span>
            </div>
        </div>

        <div class="tcche-ob-stat-card">
            <div class="tcche-ob-stat-card__icon"><span class="dashicons dashicons-yes-alt"></span></div>
            <div class="tcche-ob-stat-card__content">
                <span class="tcche-ob-stat-card__value" id="stat-conversions">--</span>
                <span class="tcche-ob-stat-card__label"><?php esc_html_e('Conversions', 'tcche-order-bump'); ?></span>
            </div>
        </div>

        <div class="tcche-ob-stat-card">
            <div class="tcche-ob-stat-card__icon"><span class="dashicons dashicons-chart-line"></span></div>
            <div class="tcche-ob-stat-card__content">
                <span class="tcche-ob-stat-card__value" id="stat-rate">--</span>
                <span class="tcche-ob-stat-card__label"><?php esc_html_e('Conversion Rate', 'tcche-order-bump'); ?></span>
            </div>
        </div>

        <div class="tcche-ob-stat-card">
            <div class="tcche-ob-stat-card__icon"><span class="dashicons dashicons-money-alt"></span></div>
            <div class="tcche-ob-stat-card__content">
                <span class="tcche-ob-stat-card__value" id="stat-revenue">--</span>
                <span class="tcche-ob-stat-card__label"><?php esc_html_e('Total Revenue', 'tcche-order-bump'); ?></span>
            </div>
        </div>

        <div class="tcche-ob-stat-card">
            <div class="tcche-ob-stat-card__icon"><span class="dashicons dashicons-cart"></span></div>
            <div class="tcche-ob-stat-card__content">
                <span class="tcche-ob-stat-card__value" id="stat-aov">--</span>
                <span class="tcche-ob-stat-card__label"><?php esc_html_e('Avg Bump Value', 'tcche-order-bump'); ?></span>
            </div>
        </div>
    </div>

    <!-- Charts -->
    <div class="tcche-ob-analytics__charts">
        <div class="tcche-ob-card tcche-ob-chart-card">
            <h2 class="tcche-ob-card__title"><?php esc_html_e('Daily Performance', 'tcche-order-bump'); ?></h2>
            <canvas id="tcche-ob-daily-chart" height="300"></canvas>
        </div>

        <div class="tcche-ob-card tcche-ob-chart-card">
            <h2 class="tcche-ob-card__title"><?php esc_html_e('Daily Revenue', 'tcche-order-bump'); ?></h2>
            <canvas id="tcche-ob-revenue-chart" height="300"></canvas>
        </div>
    </div>

    <!-- Per-Bump Table -->
    <div class="tcche-ob-card" style="margin-top:20px;">
        <h2 class="tcche-ob-card__title"><?php esc_html_e('Performance by Bump', 'tcche-order-bump'); ?></h2>
        <table class="wp-list-table widefat fixed striped" id="tcche-ob-bump-table">
            <thead>
                <tr>
                    <th><?php esc_html_e('Order Bump', 'tcche-order-bump'); ?></th>
                    <th><?php esc_html_e('Product', 'tcche-order-bump'); ?></th>
                    <th><?php esc_html_e('Impressions', 'tcche-order-bump'); ?></th>
                    <th><?php esc_html_e('Conversions', 'tcche-order-bump'); ?></th>
                    <th><?php esc_html_e('Rate', 'tcche-order-bump'); ?></th>
                    <th><?php esc_html_e('Revenue', 'tcche-order-bump'); ?></th>
                </tr>
            </thead>
            <tbody id="tcche-ob-bump-table-body">
                <tr><td colspan="6" style="text-align:center;padding:20px;"><?php esc_html_e('Loading...', 'tcche-order-bump'); ?></td></tr>
            </tbody>
        </table>
    </div>
</div>

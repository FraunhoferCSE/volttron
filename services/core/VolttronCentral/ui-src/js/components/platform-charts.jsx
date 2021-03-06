'use strict';

var React = require('react');
var PlatformChart = require('./platform-chart');
var modalActionCreators = require('../action-creators/modal-action-creators');
var platformActionCreators = require('../action-creators/platform-action-creators');
var NewChartForm = require('./new-chart-form');
var chartStore = require('../stores/platform-chart-store');

var reloadPageInterval = 1800000;

var PlatformCharts = React.createClass({
    getInitialState: function () {

        var state = {
            chartData: chartStore.getData()
        };

        this._reloadPageTimeout = setTimeout(this._reloadPage, reloadPageInterval);

        return state;
    },
    componentDidMount: function () {
        chartStore.addChangeListener(this._onChartStoreChange);
    },
    componentWillUnmount: function () {
        clearTimeout(this._reloadPageTimeout);
        chartStore.removeChangeListener(this._onChartStoreChange);
    },
    _onChartStoreChange: function () {
        this.setState({chartData: chartStore.getData()});
    },
    _reloadPage: function () {
        
        //Reload page to clear leaked memory
        if (Object.keys(this.state.chartData).length)
        {
            location.reload(true);
        }
        else
        {
            this._reloadPageTimeout = setTimeout(this._reloadPage, reloadPageInterval);
        }
    },
    _onAddChartClick: function () {

        platformActionCreators.loadChartTopics();
        modalActionCreators.openModal(<NewChartForm/>);
    },
    render: function () {

        var chartData = this.state.chartData; 

        var platformCharts = [];

        for (var key in chartData)
        {
            if (chartData[key].series.length > 0)
            {
                var platformChart = (
                    <PlatformChart key={key} 
                        chart={chartData[key]} 
                        chartKey={key} 
                        hideControls={false}/>
                );

                platformCharts.push(platformChart);
            }
        }

        if (platformCharts.length === 0)
        {
            var noCharts = <p key="no-charts" className="empty-help">No charts have been loaded.</p>
            platformCharts.push(noCharts);
        }

        return (
            <div className="view">
                <div className="absolute_anchor">
                    <div className="view__actions">
                        <button
                            className="button"
                            onClick={this._onAddChartClick}
                        >
                            Add Chart
                        </button>
                    </div>
                    <h2>Charts</h2>
                    {platformCharts}
                </div>
            </div>
        );
    },
});

module.exports = PlatformCharts;

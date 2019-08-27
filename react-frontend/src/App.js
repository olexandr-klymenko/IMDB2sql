import React from 'react';
import './App.css';
import FilmsUi from "./FilmsUi";

class App extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div>
                <header>
                    <FilmsUi/>
                </header>
            </div>
        );
    }
}

export default App;

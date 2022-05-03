import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts10Component } from './posts10.component';

describe('Posts10Component', () => {
  let component: Posts10Component;
  let fixture: ComponentFixture<Posts10Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts10Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts10Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
